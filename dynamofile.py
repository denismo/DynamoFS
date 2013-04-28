from dynamofs import BLOCK_SIZE

__author__ = 'Denis Mikhalkin'

from errno import EACCES, ENOENT, EINVAL, EEXIST, EOPNOTSUPP, EIO
from os.path import realpath
from threading import Lock
import boto.dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time
from boto.dynamodb.condition import EQ, GT
import os
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging
import sys
import cStringIO

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class DynamoFile:

    def __init__(self, path, accessor):
        self.log = logging.getLogger("dynamo-fuse-file")
        self.accessor = accessor
        self.path = path

    def write(self, data, offset):
        startBlock = offset / BLOCK_SIZE
        endBlock = (offset + len(data)) / BLOCK_SIZE
        initialBlockOffset = BLOCK_SIZE - (offset % BLOCK_SIZE)
        blockOffset = 0
        self.log.debug("write start=%d, last=%d, initial offset %d", startBlock, endBlock, initialBlockOffset)
        for block in range(startBlock, endBlock+1):
            item = self.accessor.getItemOrNone(os.path.join(self.path, str(block)), attrs=["data"])
            if item is None:
                self.log.debug("write block %d is None", block)
                item = self.accessor.newItem(attrs={
                    "path": self.path,
                    "name": str(block),
                    })
            dataSlice = data[0:initialBlockOffset] if block == startBlock else \
                data[blockOffset: blockOffset + BLOCK_SIZE]
            blockOffset += len(dataSlice)
            self.log.debug("write block %d slice length %d from offset %d", block, len(dataSlice), blockOffset)
            if "data" in item:
                self.log.debug("write block %d has data", block)
                startOffset = (offset % BLOCK_SIZE) if block == startBlock else 0
                item['data'] = item['data'][0:startOffset] + dataSlice + item['data'][startOffset + len(dataSlice):]
            else:
                self.log.debug("write block %d has NO data", block)
                item['data'] = dataSlice
            item.save()

    def read(self, offset, size):
        startBlock = offset / BLOCK_SIZE
        endBlock = (offset + size) / BLOCK_SIZE
        data = cStringIO.StringIO()
        try:
            self.log.debug("read blocks [%d .. %d]", startBlock, endBlock)
            for block in range(startBlock, endBlock+1):
                item = self.accessor.getItemOrNone(os.path.join(self.path, str(block)), attrs=["data"])
                if item is None:
                    self.log.debug("read block %d does not exist", block)
                    break
                if not "data" in item:
                    self.log.debug("read block %d has no data", block)
                    raise FuseOSError(EIO)
                itemData = item["data"]
                writeLen = min(size, BLOCK_SIZE, len(itemData))
                startOffset = (offset % BLOCK_SIZE) if block == startBlock else 0
                self.log.debug("read block %d has %d data, writing %d from %d", block, len(itemData), writeLen, startOffset)
                data.write(itemData[startOffset:startOffset + writeLen])
                size -= writeLen

            return data.getvalue()
        finally:
            data.close()
