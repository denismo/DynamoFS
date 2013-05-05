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

from dynamofs import BLOCK_SIZE

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

class MasterRecord:
    log = logging.getLogger("dynamo-fuse-master")

    def __init__(self, path, accessor, create=False, mode=None):
        self.path = path
        self.accessor = accessor
        name = os.path.basename(path)
        if name == "":
            name = "/"
        if create:
            attrs = {'name': name, 'path': os.path.dirname(path),
                     'st_blksize': BLOCK_SIZE}
            if mode & S_IFREG == S_IFREG:
                attrs["blockId"] = self.accessor.allocUniqueId()
            self.record = self.accessor.table.new_item(attrs=attrs)
            if mode & S_IFREG == S_IFREG:
                self.createFirstBlock(mode)
        else:
            try:
                self.record = self.accessor.table.get_item(os.path.dirname(path), name, attributes_to_get=None)
            except DynamoDBKeyNotFoundError:
                raise FuseOSError(ENOENT)

    def save(self):
        self.record.save()

    def getFirstBlock(self, getData=False):
        if self.firstBlock is None:
            self.firstBlock = BlockRecord(self.record["blockId"], 0, getData)
        return self.firstBlock

    def createFirstBlock(self, mode):
        l_time = int(time())
        item = self.accessor.newItem(attrs={
            "path": self.record["blockId"], "name": "0",
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
        self.record.delete()

        if self.isFile():
            block = self.getFirstBlock()
            block["st_nlink"] -= 1
            if not block["st_nlink"]:
                items = self.accessor.table.query(path, attributes_to_get=['name', 'path'])
                # TODO Pagination
                for entry in items:
                    entry.delete()
            else:
                block.save()
        elif self.isDirectory():
            raise FuseOSError(EINVAL)

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
        for block in range(startBlock, endBlock+1):
            block = self.getBlock(block, getData=True)
            if block is None:
                self.log.debug("write block %d is None", block)
                block = self.createBlock(block)
            dataSlice = data[0:initialBlockOffset] if block == startBlock else\
                data[blockOffset: blockOffset + BLOCK_SIZE]

            self.log.debug("write block %d slice length %d from offset %d", block, len(dataSlice), blockOffset)
            blockOffset += len(dataSlice)

            block.writeData((offset % BLOCK_SIZE) if block == startBlock else 0, dataSlice)
            block.save()
