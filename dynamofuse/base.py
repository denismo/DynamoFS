#!/usr/bin/env python

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

from __future__ import with_statement
from posix import F_OK, R_OK

__author__ = 'Denis Mikhalkin'

from dynamofuse.records import *
from errno import *
from os.path import realpath
from sys import argv, exit
from threading import Lock
import boto.dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from boto.exception import BotoServerError, BotoClientError
from boto.exception import DynamoDBResponseError
from stat import S_IFDIR, S_IFLNK, S_IFREG, S_ISREG, S_ISDIR, S_ISLNK
from boto.dynamodb.types import Binary
from time import time
from boto.dynamodb.condition import EQ, GT
import os
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context
import logging
import sys
import cStringIO
import itertools
import traceback

if not hasattr(__builtins__, 'bytes'):
    bytes = str

# Note: st_mode, st_gid and st_uid are at inode level
class BaseRecord:
    record = None
    accessor = None
    path = None
    log = logging.getLogger("dynamo-fuse")

    def create(self, accessor, path, attrs):
        self.accessor = accessor
        self.path = path

        name = os.path.basename(path)
        if name == "":
            name = "/"
        l_time = int(time())
        (uid, gid, unused) = fuse_get_context()
        newAttrs = {'name': name, 'path': os.path.dirname(path),
                    'type': self.__class__.__name__,
                    'st_nlink': 1,
                    'st_size': 0, 'st_ctime': l_time,
                    'st_mtime': l_time, 'st_atime': l_time,
                    'st_gid': gid, 'st_uid': uid
        }
        for k, v in attrs.items():
            newAttrs[k] = v
        item = self.accessor.table.new_item(attrs=newAttrs)
        item.put()
        self.record = item

    def init(self, accessor, path, record):
        self.accessor = accessor
        self.path = path
        self.record = record

    def delete(self):
        self.record.delete()

    def moveTo(self, newPath):
        self.cloneItem(newPath)

        self.delete()

    def cloneItem(self, path):
        attrs=dict(self.record)
        del attrs['name']
        del attrs['path']
        newItem = self.__class__()
        newItem.create(self.accessor, path, attrs)
        return newItem

    def getattr(self):
        return self.record

    def chmod(self, mode):
        block = self.record
        block['st_mode'] &= 0770000
        block['st_mode'] |= mode
        block['st_ctime'] = int(time())
        block.save()

    def chown(self, uid, gid):
        block = self.record
        block['st_uid'] = uid
        block['st_gid'] = gid
        block['st_ctime'] = int(time())
        block.save()

    def updateCTime(self):
        self.record['st_ctime'] = int(time())
        self.record.save()

    def isFile(self):
        return self.record["type"] == "File"

    def isDirectory(self):
        return self.record["type"] == "Directory"

    def isLink(self):
        return self.record["type"] == "Symlink"

    def __getitem__(self, item):
        return self.record[item]

    def __setitem__(self, key, value):
        self.record[key] = value

    def save(self):
        self.record.save()

    def access(self, mode):
        if mode == F_OK:
            return 0
