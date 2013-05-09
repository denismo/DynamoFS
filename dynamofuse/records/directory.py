from dynamofuse import BaseRecord

__author__ = 'Denis Mikhalkin'

from dynamofuse.records.block import BlockRecord
from errno import  ENOENT, EINVAL
from os.path import realpath, join, dirname, basename
from threading import Lock
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from time import time
from boto.dynamodb.condition import EQ, GT
from boto.dynamodb.types import Binary
import logging
import cStringIO
from stat import S_IFDIR, S_IFLNK, S_IFREG, S_ISREG, S_ISDIR
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import itertools

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Directory(BaseRecord):

    def getattr(self):
        self.record["st_nlink"] = 1
        self.record["st_size"] = 0
        return self.record

    def list(self):
        items = self.accessor.table.query(self.path, attributes_to_get=['name'])

        for entry in items:
            if entry['name'] == "/":
                continue # This could be the folder itself
            yield entry['name']
