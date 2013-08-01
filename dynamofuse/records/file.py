#    Dynamo-Fuse - POSIX-compliant distributed FUSE file system with AWS DynamoDB as backend
#    Copyright (C) 2013 Denis Mikhalkin
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

__author__ = 'Denis Mikhalkin'

from posix import R_OK, X_OK, W_OK
from dynamofuse.records.block import BlockRecord
from dynamofuse.base import BaseRecord, DELETED_LINKS
from errno import  ENOENT, EINVAL, EPERM
import os
from os.path import realpath, join, dirname, basename
from threading import Lock
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from time import time
from boto.dynamodb.condition import EQ, GT
from boto.dynamodb.types import Binary
import logging
import cStringIO
from stat import *
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context
import itertools
import uuid

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class File(BaseRecord):
    log = logging.getLogger("dynamo-fuse-master")

    def create(self, accessor, path, attrs):
        self.path = path
        self.accessor = accessor

        assert 'st_mode' in attrs

        if not 'st_blksize' in attrs:
            attrs['st_blksize'] = accessor.BLOCK_SIZE

        if not "blockId" in attrs:
            attrs["blockId"] = str(self.accessor.allocUniqueId())
            attrs["st_ino"] = int(attrs["blockId"])

        BaseRecord.create(self, accessor, path, attrs)

    def getFirstBlock(self, getData=False):
        return self.record

    def getBlock(self, blockNum, getData=False, forUpdate=False):
        return BlockRecord(self.accessor, os.path.join(self.record["blockId"], str(blockNum))).read(getData, forUpdate)

    def createBlock(self, blockNum):
        return BlockRecord(self.accessor, os.path.join(self.record["blockId"], str(blockNum))).create(attrs={
            "blockId": self.record["blockId"], "blockNum": blockNum
        })

    def updateCTime(self):
        block = self.getFirstBlock()
        block['st_ctime'] = max(block['st_ctime'], int(time()))
        block.save()

    def getRecord(self):
        return self.record

    def isDeleted(self):
        return 'deleted' in self.record and self.record['deleted']

    def link(self):
        # TODO: Locking? What if the file is deleted?
        block = self.getFirstBlock()
        block.add_attribute("st_nlink", 1)
        block['st_ctime'] = max(block['st_ctime'], int(time()))
        block.save()

    ################# OPERATIONS ##########################

    def getattr(self):
        block = self.getFirstBlock()
        block["st_blocks"] = (block["st_size"] + self.record["st_blksize"] - 1) / self.record["st_blksize"]
        block["st_ino"] = int(self.record["blockId"])
        return block

    def delete(self):
        with self.takeLock():
            self.deleteFile()

    def deleteFile(self, linked=False):
        block = self.getFirstBlock()
        self.log.debug("Delete file, linked=%s, links=%d", linked, block["st_nlink"])
        block.add_attribute("st_nlink", -1)
        block['st_ctime'] = max(block['st_ctime'], int(time()))
        delete = not linked and ('deleted' in block and not block['deleted'] or not 'deleted' in block)
        block['deleted'] = not linked or ('deleted' in block and block['deleted'])
        if block["st_nlink"] == 1:
            self.log.debug("No more links - deleting records")
            items = self.accessor.blockTablev2.query(blockId__eq=self.record["blockId"], attributes=['blockId', 'blockNum'])
            for entry in items:
                entry.delete()

            BaseRecord.delete(self)
        else:
#            resp = block.save(return_values='ALL_NEW')
#            self.log.debug('After deleting: ' + str(resp))
            if delete:
                block['st_nlink'] -= 1
                self.moveTo(os.path.join("/" + DELETED_LINKS, uuid.uuid4().hex), forceUpdate=True)
            else:
                block.save()

    def write(self, data, offset):
        with self.takeLock():
            self._write(data, offset)
            block = self.getFirstBlock()
            block["st_size"] = max(block["st_size"], offset + len(data))
            block['st_ctime'] = max(block['st_ctime'], int(time()))
            block['st_mtime'] = max(block['st_mtime'], int(time()))
            block.save()

            return len(data)

    def _write(self, data, offset):
        startBlock = offset / self.accessor.BLOCK_SIZE
        endBlock = (offset + len(data) - 1) / self.accessor.BLOCK_SIZE
        initialBlockOffset = self.accessor.BLOCK_SIZE - (offset % self.accessor.BLOCK_SIZE)
        blockOffset = 0
        self.log.debug("write start=%d, last=%d, initial offset %d", startBlock, endBlock, initialBlockOffset)
        for blockNum in range(startBlock, endBlock + 1):
            try:
                block = self.getBlock(blockNum, getData=True)
            except FuseOSError, fe:
                if fe.errno == ENOENT:
                    self.log.debug("write block %d is None", blockNum)
                    block = self.createBlock(blockNum)
                else:
                    raise
            dataSlice = data[0:initialBlockOffset] if blockNum == startBlock else\
            data[blockOffset: blockOffset + self.accessor.BLOCK_SIZE]

            self.log.debug("write block %d slice length %d from offset %d", blockNum, len(dataSlice), blockOffset)
            blockOffset += len(dataSlice)

            block.writeData((offset % self.accessor.BLOCK_SIZE) if blockNum == startBlock else 0, dataSlice)
            block.save()

    def read(self, offset, size):
        startBlock = offset / self.accessor.BLOCK_SIZE
        if offset+size > self.record["st_size"]:
            size = self.record["st_size"] - offset
        endBlock = (offset + size - 1) / self.accessor.BLOCK_SIZE
        data = cStringIO.StringIO()
        try:
            self.log.debug("read blocks [%d .. %d]", startBlock, endBlock)
            for block in range(startBlock, endBlock+1):
                item = self.getBlock(block, getData=True)
                if item is None:
                    self.log.debug("read block %d does not exist", block)
                    break
                if not "data" in item:
                    self.log.debug("read block %d has no data", block)
                    break
                itemData = item["data"].value
                writeLen = min(size, self.accessor.BLOCK_SIZE, len(itemData))
                startOffset = (offset % self.accessor.BLOCK_SIZE) if block == startBlock else 0
                self.log.debug("read block %d has %d data, write %d from %d", block, len(itemData), writeLen,
                    startOffset)
                data.write(itemData[startOffset:startOffset + writeLen])
                size -= writeLen

            return data.getvalue()
        finally:
            data.close()

    def moveTo(self, newPath, forceUpdate=False):
        # Files can be hard-linked. When moved, they will update the targets of their hard-links to point to new name (as hard links are actually by name)
        self.cloneItem(newPath)

        # TODO: A problem - in order to perform this query we have to run it against hash_key + "link" which is impossible with current design
        if self.record["st_nlink"] > 1 or forceUpdate:
            self.log.debug("Retargeting links from %s to %s" % (self.path, newPath))
            items = self.accessor.table.scan({"link":EQ(self.path)})
            for link in items:
                link["link"] = newPath
                link.save()

            # This will force delete the block
#            self.record["st_nlink"] = 1

        self.record.delete()
        self.deleted = True

    def truncate(self, length, fh=None):
        with self.takeLock():
            lastBlock = length / self.accessor.BLOCK_SIZE
            l_time = int(time())

            items = self.accessor.blockTablev2.query(blockId__eq=self.record["blockId"], blockNum__gt=lastBlock, attributes=["blockId", "blockNum"])
            for entry in items:
                entry.delete()

            if length:
                try:
                    lastItem = self.getBlock(lastBlock, getData=True)
                    if lastItem is not None and "data" in lastItem:
                        lastItem['data'] = Binary(lastItem['data'].value[0:(length % self.accessor.BLOCK_SIZE)])
                        lastItem.save()
                except FuseOSError, fe:
                    # Block is missing - so nothing to update
                    if fe.errno == ENOENT:
                        pass
                    else:
                        raise fe

            item = self.getFirstBlock()
            item['st_size'] = length
            item['st_ctime'] = max(l_time, item['st_ctime'])
            item['st_mtime'] = max(l_time, item['st_mtime'])
            item.save()
