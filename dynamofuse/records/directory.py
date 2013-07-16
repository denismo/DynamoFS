from dynamofuse.base import BaseRecord

__author__ = 'Denis Mikhalkin'

from errno import  ENOENT, EINVAL
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
from posix import R_OK, X_OK, W_OK

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Directory(BaseRecord):

    def getattr(self):
        self.record["st_nlink"] = 1
        self.record["st_size"] = 0
        return self.record

    def list(self):
        items = self.accessor.table.query(self.path, attributes_to_get=['name'], consistent_read=True)

        for entry in items:
            if entry['name'] == "/":
                continue # This could be the folder itself
            yield entry['name']

    def moveTo(self, newPath):
        self.cloneItem(newPath, ['type', 'st_nlink', 'st_size', 'st_ino', 'st_mode'])

        self.moveDirectory(newPath)

        self.delete()

    def moveDirectory(self, new):
        for entry in self.accessor.readdir(self.path):
            if entry == "." or entry == "..": continue
            self.accessor.rename(os.path.join(self.path, entry), os.path.join(new, entry))

    def isEmpty(self):
        return len(list(self.accessor.table.query(self.path, attributes_to_get=['name'], consistent_read=True))) == 0

    def access(self, mode):
        block = self.record
        st_mode = block['st_mode']
        st_uid = block['st_uid']
        st_gid = block['st_gid']
        if not self.modeAccess(mode, st_mode, st_uid, st_gid): return 0

        return BaseRecord.access(self, mode)
