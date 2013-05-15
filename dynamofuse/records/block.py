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

from errno import EACCES, ENOENT, EINVAL, EEXIST, EOPNOTSUPP, EIO, EAGAIN
from os.path import realpath
from threading import Lock
import boto.dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time
from boto.dynamodb.condition import EQ, GT
from boto.dynamodb.types import Binary
import os
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging
import sys
import cStringIO

if not hasattr(__builtins__, 'bytes'):
    bytes = str

blockCache = dict()
blockLog = logging.getLogger("dynamo-fuse-block")

class BlockRecord:
    BLOCK_ATTRS = ["st_size", "st_nlink", "st_mtime", "st_atime", "st_ctime", "st_mode", 'st_uid', 'st_gid', 'st_blksize', 'version']
    BLOCK_ALL_ATTRS = ["data"] + BLOCK_ATTRS

    log = logging.getLogger("dynamo-fuse-block")
    item = None
    accessor = None
    path = None

    def __init__(self, accessor, path):
        self.accessor = accessor
        self.path = path

    def read(self, getData=False):
        self.item = BlockRecord.getBlockItem(self.accessor, self.path, getData)
        return self

    def create(self, attrs):
        self.item = self.accessor.newItem(attrs)
        self.item["version"] = 1
        self.item.put()
        BlockRecord.cacheItem(self.path, self.item, self.item["version"])
        return self

    def __getitem__(self, key):
        return self.item[key]

    def __setitem__(self, key, value):
        self.item[key] = value
        return value

    def __contains__(self, item):
        return item in self.item

    def save(self):
        self.item.add_attribute("version", 1)
        self.item.save()
        BlockRecord.cacheItem(self.path, self.item, self.item["version"] + 1)

    def writeData(self, startOffset, dataSlice):
        if "data" in self.item:
            self.log.debug("write block %s has data", self.item["name"])
            itemData = self.item["data"].value
            self.item['data'] = Binary(itemData[0:startOffset] + dataSlice + itemData[startOffset + len(dataSlice):])
        else:
            self.log.debug("write block %s has NO data", self.item["name"])
            self.item['data'] = Binary(dataSlice)

    @staticmethod
    def getBlockItem(accessor, path, getData=False):
        blockItem = accessor.getItemOrNone(path, attrs=(BlockRecord.BLOCK_ALL_ATTRS if getData else BlockRecord.BLOCK_ATTRS))
        cachedBlockItem = BlockRecord.getCachedBlockItem(path)
        if not blockItem:
            if cachedBlockItem and (getData and "data" in cachedBlockItem or not getData):
                blockLog.debug('Returning cached block item for %s', path)
                return cachedBlockItem
            else:
                blockLog.debug('Unable to find block or cached block for %s', path)
                raise FuseOSError(ENOENT)
        if cachedBlockItem and blockItem["version"] < cachedBlockItem["version"] and (getData and "data" in cachedBlockItem or not getData):
            blockLog.debug('Returning cached block item for %s', path)
            return cachedBlockItem
        return blockItem

    @staticmethod
    def getCachedBlockItem(path):
        try:
            return blockCache[path]
        except:
            return None

    @staticmethod
    def cacheItem(path, item, version):
        blockLog.debug('Caching block for %s', path)
        item["version"] = version
        blockCache[path] = item