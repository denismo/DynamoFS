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
from datetime import datetime, timedelta
from boto.dynamodb.condition import EQ, GT
from boto.dynamodb.types import Binary
import os
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging
from collections import deque
import sys
import cStringIO

if not hasattr(__builtins__, 'bytes'):
    bytes = str

blockCache = dict()
blockHistory = deque()
blockLog = logging.getLogger("dynamo-fuse-block ")
BLOCK_CACHE_ENABLED=False

class BlockRecord:
    BLOCK_ATTRS = ['version', "blockId", "blockNum"]
    BLOCK_ALL_ATTRS = ["data"] + BLOCK_ATTRS

    log = logging.getLogger("dynamo-fuse-block ")
    item = None
    accessor = None
    path = None

    def __init__(self, accessor, path):
        self.accessor = accessor
        self.path = path

    def read(self, getData=False, forUpdate=False):
        self.item = BlockRecord.getBlockItem(self.accessor, self.path, getData, forUpdate)
        return self

    def create(self, attrs):
        attrs["version"] = 1
        self.item = self.accessor.blockTable.new_item(attrs=attrs)
        self.item.put(expected_value={'blockId':False, 'blockNum':False})
        if BLOCK_CACHE_ENABLED: BlockRecord.cacheItem(self.path, self.item, self.item["version"])
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
        if BLOCK_CACHE_ENABLED:
            # Delete this properties before they are saved into DynamoDB. They will be restored next time the item is cached
            if "updateTime" in self.item: del self.item["updateTime"]
            if "fullPath" in self.item: del self.item["fullPath"]
        self.item.save()
        if BLOCK_CACHE_ENABLED:
            BlockRecord.cacheItem(self.path, self.item, self.item["version"] + 1)

    def writeData(self, startOffset, dataSlice):
        if "data" in self.item:
            self.log.debug("write block %s has data", self.path)
            itemData = self.item["data"].value
            self.item['data'] = Binary(itemData[0:startOffset] + dataSlice + itemData[startOffset + len(dataSlice):])
        else:
            self.log.debug("write block %s has NO data", self.path)
            self.item['data'] = Binary(dataSlice)

    @staticmethod
    def getBlockItem(accessor, path, getData=False, forUpdate=False):
        try:
            blockItem = accessor.blockTable.get_item(os.path.dirname(path), int(os.path.basename(path)),
                attributes_to_get=(BlockRecord.BLOCK_ALL_ATTRS if getData else BlockRecord.BLOCK_ATTRS))
        except DynamoDBKeyNotFoundError:
            blockItem = None
        cachedBlockItem = BlockRecord.getCachedBlockItem(path, forUpdate) if BLOCK_CACHE_ENABLED else None
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
        else:
            try:
                del blockCache[path]
            except:
                pass
            try:
                blockHistory.remove(blockItem)
            except:
                pass
            return blockItem

    @staticmethod
    def getCachedBlockItem(path, forUpdate=False):
        if forUpdate:
            item = blockCache.pop(path, None)
            # Need to remove the item because the next update will put it in at different offset
            if item:
                try:
                    blockHistory.remove(item)
                except:
                    pass
            return item
        else:
            return blockCache.get(path, None)

    @staticmethod
    def cacheItem(path, item, version):
        blockLog.debug('Caching block for %s', path)
        item["version"] = version
        item["updateTime"] = datetime.now()
        item["fullPath"] = path
        blockCache[path] = item
        blockHistory.append(item)
        BlockRecord.expellExpiredBlocks()

    @staticmethod
    def expellExpiredBlocks():
        now = datetime.now()
        EXPELL_DELTA = timedelta(seconds=2)
        while len(blockHistory) > 0 and (now - blockHistory[0]["updateTime"]) > EXPELL_DELTA:
            block = blockHistory.popleft()
            try:
                del blockCache[block["fullPath"]]
            except:
                pass