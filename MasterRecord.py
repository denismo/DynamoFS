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

from BlockRecord import BLOCK_SIZE

from errno import EACCES, ENOENT, EINVAL, EEXIST, EOPNOTSUPP, EIO, EAGAIN
from os.path import realpath
from threading import Lock
import boto.dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from time import time
from boto.dynamodb.condition import EQ, GT
from boto.dynamodb.types import Binary
import os
import logging
import sys
import cStringIO
from BlockRecord import BlockRecord
from stat import S_IFDIR, S_IFLNK, S_IFREG, S_ISREG, S_ISDIR
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import itertools

if not hasattr(__builtins__, 'bytes'):
    bytes = str

# TODO Split class into individual implementations for file, directory and link
class MasterRecord:
    log = logging.getLogger("dynamo-fuse-master")

    def __init__(self, path, accessor, create=False, mode=None, attrs=None):
        self.path = path
        self.accessor = accessor
        name = os.path.basename(path)
        if name == "":
            name = "/"
        if create:
            attrs = {'name': name, 'path': os.path.dirname(path),
                     'st_blksize': BLOCK_SIZE} if attrs is None else attrs
            if mode is not None and not "type" in attrs:
                if (mode & S_IFREG) == S_IFREG: attrs['type'] = 'File'
                if (mode & S_IFDIR) == S_IFDIR: attrs['type'] = 'Directory'
                if (mode & S_IFLNK) == S_IFLNK: attrs['type'] = 'Symlink'

            createBlock = False # Only create block if it is not already created
            if (mode & S_IFREG) == S_IFREG and not "blockId" in attrs:
                attrs["blockId"] = str(self.accessor.allocUniqueId())
                createBlock = True
            if (mode & S_IFDIR) == S_IFDIR and not "st_ctime" in attrs:
                l_time = int(time())
                attrs['st_ctime'] = l_time
                attrs['st_mtime'] = l_time
                attrs['st_atime'] = l_time
                attrs['st_mode'] = mode

            self.record = self.accessor.table.new_item(attrs=attrs)
            self.record.put()
            if createBlock:
                self.createFirstBlock(mode)
        else:
            try:
                self.record = self.accessor.table.get_item(os.path.dirname(path), name)
            except DynamoDBKeyNotFoundError:
                raise FuseOSError(ENOENT)

    @staticmethod
    def create(table, hash, range, item):
        if "type" in item:
            if item['type'] == "Symlink":
                return SymlinkRecord(item, table)
            if item['type'] == 'Directory':
                return DirectoryRecord(item, table)
            if item['type'] == 'File':
                return MasterRecord(item, table)

    def __getitem__(self, item):
        return self.record[item]

    def __setitem__(self, key, value):
        self.record[key] = value

    def save(self):
        self.record.save()

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

    def isFile(self):
        return self.record["type"] == "File"

    def isDirectory(self):
        return self.record["type"] == "Directory"

    def isLink(self):
        return self.record["type"] == "Symlink"

    def delete(self):
        if self.isFile():
            block = self.getFirstBlock()
            block["st_nlink"] -= 1
            if not block["st_nlink"]:
                items = self.accessor.table.query(self.record["blockId"], attributes_to_get=['name', 'path'])
                # TODO Pagination
                for entry in items:
                    entry.delete()
            else:
                block.save()
            self.record.delete()
        elif self.isDirectory():
            raise FuseOSError(EINVAL)
        else:
            self.record.delete()

    def write2(self, data, offset):
        startBlock = offset / BLOCK_SIZE
        endBlock = (offset + len(data)-1) / BLOCK_SIZE
        initialBlockOffset = BLOCK_SIZE - (offset % BLOCK_SIZE)
        blockOffset = 0
        self.log.debug("write start=%d, last=%d, initial offset %d", startBlock, endBlock, initialBlockOffset)
        for block in range(startBlock, endBlock+1):
            item = self.accessor.getItemOrNone(os.path.join(self.path, str(block)), attrs=["data"])
            if item is None:
                self.log.debug("write block %d is None", block)
                if not block:
                    # First block must keep the link count and times
                    raise "First block cannot be empty for " + self.path
                else:
                    item = self.accessor.newItem(attrs={"path": self.path, "name": str(block) })
            dataSlice = data[0:initialBlockOffset] if block == startBlock else\
            data[blockOffset: blockOffset + BLOCK_SIZE]
            self.log.debug("write block %d slice length %d from offset %d", block, len(dataSlice), blockOffset)
            blockOffset += len(dataSlice)
            if "data" in item:
                self.log.debug("write block %d has data", block)
                itemData = item["data"].value
                startOffset = (offset % BLOCK_SIZE) if block == startBlock else 0
                item['data'] = Binary(itemData[0:startOffset] + dataSlice + itemData[startOffset + len(dataSlice):])
            else:
                self.log.debug("write block %d has NO data", block)
                item['data'] = Binary(dataSlice)
            item.save()

    def write(self, data, offset):
        startBlock = offset / BLOCK_SIZE
        endBlock = (offset + len(data)-1) / BLOCK_SIZE
        initialBlockOffset = BLOCK_SIZE - (offset % BLOCK_SIZE)
        blockOffset = 0
        self.log.debug("write start=%d, last=%d, initial offset %d", startBlock, endBlock, initialBlockOffset)
        for blockNum in range(startBlock, endBlock+1):
            block = self.getBlock(blockNum, getData=True)
            if block is None:
                self.log.debug("write block %d is None", blockNum)
                block = self.createBlock(blockNum)
            dataSlice = data[0:initialBlockOffset] if blockNum == startBlock else\
                data[blockOffset: blockOffset + BLOCK_SIZE]

            self.log.debug("write block %d slice length %d from offset %d", blockNum, len(dataSlice), blockOffset)
            blockOffset += len(dataSlice)

            block.writeData((offset % BLOCK_SIZE) if blockNum == startBlock else 0, dataSlice)
            block.save()

    def read(self, offset, size):
        startBlock = offset / BLOCK_SIZE
        endBlock = (offset + size-1) / BLOCK_SIZE
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
                writeLen = min(size, BLOCK_SIZE, len(itemData))
                startOffset = (offset % BLOCK_SIZE) if block == startBlock else 0
                self.log.debug("read block %d has %d data, writing %d from %d", block, len(itemData), writeLen, startOffset)
                data.write(itemData[startOffset:startOffset + writeLen])
                size -= writeLen

            return data.getvalue()
        finally:
            data.close()

    def moveTo(self, newPath):
        self.cloneItem(newPath)

        if self.isDirectory():
            self.moveDirectory(newPath)

        self.delete()

    def cloneItem(self, path):
        attrs=dict((k, self.record[k]) for k in self.record.keys())
        attrs["path"] = os.path.dirname(path)
        attrs["name"] = os.path.basename(path)

        # Uses existing blockId
        newItem = MasterRecord(path, self.accessor, create=True, mode=self.getFirstBlock()["st_mode"], attrs=attrs)
        newBlock = newItem.getFirstBlock()
        newBlock["st_ctime"] = int(time())
        newBlock["st_nlink"] += 1
        newBlock.save()

    def moveDirectory(self, new):
        for entry in self.accessor.readdir(self.path):
            if entry == "." or entry == "..": continue
            self.accessor.rename(os.path.join(self.path, entry), os.path.join(new, entry))

    def truncate(self, length, fh=None):
        lastBlock = length / BLOCK_SIZE

        items = self.accessor.table.query(hash_key=self.path, range_key_condition=GT(str(lastBlock)), attributes_to_get=['name', "path"])
        # TODO Pagination
        for entry in items:
            entry.delete()

        if length:
            lastItem = self.getBlock(lastBlock, getData=True)
            if lastItem is not None and "data" in lastItem:
                lastItem['data'] = Binary(lastItem['data'].value[0:(length % BLOCK_SIZE)])
                lastItem.save()

        item = self.getFirstBlock(getData=False)
        item['st_size'] = length
        item.save()
