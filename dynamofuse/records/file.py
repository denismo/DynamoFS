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

from dynamofuse.records.block import BlockRecord
from dynamofuse import BaseRecord
from errno import  ENOENT, EINVAL
from os.path import realpath, join, dirname, basename
from threading import Lock
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from time import time
from boto.dynamodb.condition import EQ, GT
from boto.dynamodb.types import Binary
import logging
import cStringIO
from stat import S_IFDIR, S_IFLNK, S_IFREG, S_ISREG, S_ISDIR, S_ISLNK
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import itertools

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

        createBlock = False
        if not "blockId" in attrs:
            attrs["blockId"] = str(self.accessor.allocUniqueId())
            createBlock = True

        BaseRecord.create(self, accessor, path, attrs)

        if createBlock:
            self.createFirstBlock(attrs['st_mode'])

    def getFirstBlock(self, getData=False):
        if self.isDirectory() or self.isLink():
            return self.record

        if not hasattr(self, 'firstBlock'):
            self.firstBlock = BlockRecord(self.accessor.getItemOrThrow(os.path.join(self.record["blockId"], "0"),
                attrs=(["data", "st_size", "st_nlink", "st_mtime", "st_atime", "st_ctime", "st_mode"] if getData else ["st_size", "st_nlink", "st_mtime", "st_atime", "st_ctime", "st_mode"])))

        return self.firstBlock

    def getBlock(self, blockNum, getData=False):
        return BlockRecord(self.accessor.getItemOrThrow(os.path.join(self.record["blockId"], str(blockNum)),
            attrs=(["data"] if getData else [])))

    def createBlock(self, blockNum):
        assert blockNum != 0, "First block is a special block"
        item = self.accessor.newItem(attrs={
            "path": self.record["blockId"], "name": str(blockNum)
        })
        item.put()

    def createFirstBlock(self, mode):
        l_time = int(time())
        item = self.accessor.newItem(attrs={
            "path": self.record["blockId"],
            "name": "0",
            "st_nlink": 1,
            "st_mtime": l_time,
            "st_atime": l_time,
            "st_ctime": l_time,
            "st_mode":  mode,
            "st_size": 0
        })
        item.put()


    ################# OPERATIONS ##########################
    def chmod(self, mode):
        block = self.getFirstBlock()
        block['st_mode'] &= 0770000
        block['st_mode'] |= mode
        block.save()

    def chown(self, uid, gid):
        block = self.getFirstBlock()
        block['st_uid'] = uid
        block['st_gid'] = gid
        block.save()

    def getattr(self):
        block = self.getFirstBlock()
        block["st_blocks"] = (block["st_size"] + self.record["st_blksize"]-1)/self.record["st_blksize"]
        return block.item

    def utimens(self, atime, mtime):
        block = self.getFirstBlock()
        block['st_atime'] = atime
        block['st_mtime'] = mtime
        block.save()

    def delete(self):
        block = self.getFirstBlock()
        block["st_nlink"] -= 1
        if not block["st_nlink"]:
            items = self.accessor.table.query(self.record["blockId"], attributes_to_get=['name', 'path'])
            # TODO Pagination
            for entry in items:
                entry.delete()
        else:
            block.save()

        BaseRecord.delete(self)

    def write(self, data, offset):
        self._write(data, offset)
        block = self.getFirstBlock()
        block["st_size"] = max(block["st_size"], offset + len(data))
        block.save()

        return len(data)

    def _write(self, data, offset):
        startBlock = offset / self.accessor.BLOCK_SIZE
        endBlock = (offset + len(data)-1) / self.accessor.BLOCK_SIZE
        initialBlockOffset = self.accessor.BLOCK_SIZE - (offset % self.accessor.BLOCK_SIZE)
        blockOffset = 0
        self.log.debug("write start=%d, last=%d, initial offset %d", startBlock, endBlock, initialBlockOffset)
        for blockNum in range(startBlock, endBlock+1):
            block = self.getBlock(blockNum, getData=True)
            if block is None:
                self.log.debug("write block %d is None", blockNum)
                block = self.createBlock(blockNum)
            dataSlice = data[0:initialBlockOffset] if blockNum == startBlock else\
                data[blockOffset: blockOffset + self.accessor.BLOCK_SIZE]

            self.log.debug("write block %d slice length %d from offset %d", blockNum, len(dataSlice), blockOffset)
            blockOffset += len(dataSlice)

            block.writeData((offset % self.accessor.BLOCK_SIZE) if blockNum == startBlock else 0, dataSlice)
            block.save()

    def read(self, offset, size):
        startBlock = offset / self.accessor.BLOCK_SIZE
        endBlock = (offset + size-1) / self.accessor.BLOCK_SIZE
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
                self.log.debug("read block %d has %d data, writing %d from %d", block, len(itemData), writeLen, startOffset)
                data.write(itemData[startOffset:startOffset + writeLen])
                size -= writeLen

            return data.getvalue()
        finally:
            data.close()

    def cloneItem(self, path):
        # Our record does not contain st_mode - only first block does
        self.record['st_mode'] = self.getFirstBlock()["st_mode"]
        newItem = BaseRecord.cloneItem(self, path)

        newBlock = newItem.getFirstBlock()
        newBlock["st_ctime"] = int(time())
        newBlock["st_nlink"] += 1
        newBlock.save()

    def truncate(self, length, fh=None):
        lastBlock = length / self.accessor.BLOCK_SIZE

        items = self.accessor.table.query(hash_key=self.path, range_key_condition=GT(str(lastBlock)), attributes_to_get=['name', "path"])
        # TODO Pagination
        for entry in items:
            entry.delete()

        if length:
            lastItem = self.getBlock(lastBlock, getData=True)
            if lastItem is not None and "data" in lastItem:
                lastItem['data'] = Binary(lastItem['data'].value[0:(length % self.accessor.BLOCK_SIZE)])
                lastItem.save()

        item = self.getFirstBlock(getData=False)
        item['st_size'] = length
        item.save()
