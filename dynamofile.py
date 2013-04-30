from dynamofs import BLOCK_SIZE

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

class DynamoFile:

    def __init__(self, item, accessor):
        self.log = logging.getLogger("dynamo-fuse-file")
        self.accessor = accessor
        self.path = item["blockId"]

    def write(self, data, offset):
        startBlock = offset / BLOCK_SIZE
        endBlock = (offset + len(data)-1) / BLOCK_SIZE
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

    def read(self, offset, size):
        startBlock = offset / BLOCK_SIZE
        endBlock = (offset + size-1) / BLOCK_SIZE
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
                itemData = item["data"].value
                writeLen = min(size, BLOCK_SIZE, len(itemData))
                startOffset = (offset % BLOCK_SIZE) if block == startBlock else 0
                self.log.debug("read block %d has %d data, writing %d from %d", block, len(itemData), writeLen, startOffset)
                data.write(itemData[startOffset:startOffset + writeLen])
                size -= writeLen

            return data.getvalue()
        finally:
            data.close()

    def exclusiveLock(self):
        return DynamoLock(self.path, self.accessor)


class DynamoLock:
    log = logging.getLogger("dynamo-fuse-lock")

    def __init__(self, path, accessor):
        self.path = path
        self.accessor = accessor

    def __enter__(self):
        self.log.debug("Acquiring exclusive lock on %s", self.path)
        item = self.accessor.newItem(attrs={
            "path": self.path,
            "name": "ex_lock"
        })
        item.add_attribute("ex_lock", 1)
        res = item.save(return_values="ALL_NEW")
        self.log.debug("Lock result: %s", repr(res))
        if res["Attributes"]["ex_lock"] == 1:
            # Got the lock
            self.log.debug("Got the lock on %s", self.path)
            pass
        else:
            # TODO Wait
            self.log.debug("CANNOT lock %s, counter %d ", self.path, res["Attributes"]["ex_lock"])
            self.__exit__()
            raise FuseOSError(EAGAIN)

    def __exit__(self, t, v, tb):
        self.log.debug("Releasing exclusive lock on %s", self.path)
        item = self.accessor.newItem(attrs={
            "path": self.path,
            "name": "ex_lock"
        })
        item.add_attribute("ex_lock", -1)
        item.save()