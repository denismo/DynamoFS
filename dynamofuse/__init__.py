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
from dynamofs2 import BLOCK_SIZE
import dynamofuse
from dynamofuse.records.file import File

__author__ = 'Denis Mikhalkin'

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
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging
import sys
import cStringIO
import itertools
import traceback

if not hasattr(__builtins__, 'bytes'):
    bytes = str

ALL_ATTRS = None

class BotoExceptionMixin:
    log = logging.getLogger("dynamo-fuse")
    def __call__(self, op, path, *args):
        try:
            ret = getattr(self, op)(path, *args)
            self.log.debug("<- %s: %s", op, repr(ret))
            return ret
        except BotoServerError, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log.error("<- %s: %s", op, traceback.format_exception(exc_type, exc_value, exc_traceback))
            raise FuseOSError(EIO)
        except BotoClientError, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log.error("<- %s: %s", op, traceback.format_exception(exc_type, exc_value, exc_traceback))
            raise FuseOSError(EIO)
        except DynamoDBResponseError, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log.error("<- %s: %s", op, traceback.format_exception(exc_type, exc_value, exc_traceback))
            raise FuseOSError(EIO)
        except FuseOSError, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log.error("<- %s: %s", op, traceback.format_exception(exc_type, exc_value, exc_traceback))
            raise e
        except BaseException, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log.error("<- %s: %s", op, traceback.format_exception(exc_type, exc_value, exc_traceback))
            raise FuseOSError(EIO)


class DynamoFS(BotoExceptionMixin, Operations):
    BLOCK_SIZE = 32768

    def __init__(self, region, tableName):
        self.log = logging.getLogger("dynamo-fuse")
        self.tableName = tableName
        self.conn = boto.dynamodb.connect_to_region(region, aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
        self.table = self.conn.get_table(tableName)
        self.counter = itertools.count()
        self.__createRoot()

    def init(self, conn):
        self.log.debug("init")

    def __createRoot(self):
        if not self.table.has_item("/", "/"):
            self.mkdir("/", 0755)

    # TODO Implement access
    def access(self, path, amode):
        raise FuseOSError(EAFNOSUPPORT)

    def chmod(self, path, mode):
        self.log.debug("chmod(%s, mode=%d)", path, mode)

        self.getRecordOrThrow(path).chmod(mode)
        return 0

    def chown(self, path, uid, gid):
        self.log.debug("chown(%s, uid=%d, gid=%d)", path, uid, gid)

        self.getRecordOrThrow(path).chown(uid, gid)
        return 0

    def getattr(self, path, fh=None):
        self.log.debug("getattr(%s)", path)

        return self.getRecordOrThrow(path).getattr()

    def open(self, path, flags):
        self.log.debug("open(%s, flags=0x%x)", path, flags)
        # TODO read/write locking? Permission check?
        self.checkFileExists(path)
        return self.allocId()

    # TODO Continue refactoring
    def utimens(self, path, times=None):
        self.log.debug("utimens(%s)", path)
        now = int(time())
        atime, mtime = times if times else (now, now)

        item = self.getRecordOrThrow(path)
        if not item.isFile():
            raise FuseOSError(EINVAL)

        item.utimens(atime, mtime)

    def opendir(self, path):
        self.log.debug("opendir(%s)", path)
        self.checkFileExists(path)
        return self.allocId()

    def readdir(self, path, fh=None):
        self.log.debug("readdir(%s)", path)
        # Verify the directory exists
        dir = self.getRecordOrThrow(path)

        return ['.', '..'] + dir.list()

    def mkdir(self, path, mode):
        self.log.debug("mkdir(%s)", path)
        self.create(path, mode, "Directory")

    def rmdir(self, path):
        self.log.debug("rmdir(%s)", path)

        item = self.getRecordOrThrow(path)

        if not item.isDirectory():
            raise FuseOSError(EINVAL)

        if len(item.list()) > 0:
            raise FuseOSError(ENOTEMPTY)

        item.delete()

    def rename(self, old, new):
        self.log.debug("rename(%s, %s)", old, new)
        if old == new: return
        if old == "/" or new == "/":
            raise FuseOSError(EINVAL)

        item = self.getRecordOrThrow(old)
        newItem = self.getItemOrNone(new, attrs=[])
        if not newItem is None:
            raise FuseOSError(EEXIST)
        else:
            item.moveTo(new)

    def readlink(self, path):
        self.log.debug("readlink(%s)", path)

        item = self.getRecordOrThrow(path)

        if not item.isLink():
            raise FuseOSError(EINVAL)

        return item["symlink"]

    def symlink(self, target, source):
        self.log.debug("symlink(%s, %s)", target, source)
        if len(target) > 1024:
            raise FuseOSError(ENAMETOOLONG)

        item = self.getItemOrNone(target, attrs=[])
        if item is not None:
            raise FuseOSError(EEXIST)

        self.createRecord(target, "Symlink", attrs={'symlink': source})

        # TODO: Update parent directory time
        return 0

    def create(self, path, mode, fh=None):
        self.log.debug("create(%s, %d)", path, mode)
        if len(path) > 1024:
            raise FuseOSError(ENAMETOOLONG)

        item = self.getItemOrNone(path, attrs=[])
        if item is not None:
            raise FuseOSError(EEXIST)

        # TODO: Update parent directory time
        if mode & S_IFDIR == 0:
            type = "Directory"
        elif mode & S_IFLNK == 0:
            type = "Symlink"
        else: type = "File"

        self.createRecord(path, type, attrs={'st_mode': mode})

        return self.allocId()

    def statfs(self, path):
        self.log.debug("statfs(%s)", path)
        return dict(
            f_bsize=BLOCK_SIZE,
            f_frsize=BLOCK_SIZE,
            f_blocks=(sys.maxint - 1),
            f_bfree=(sys.maxint - 2),
            f_bavail=(sys.maxint - 2),
            f_files=self.fileCount(),
            f_ffree=sys.maxint - 1,
            f_favail=sys.maxint - 1,
            f_fsid=0,
            f_flag=0,
            f_namemax=1024
        )

    def destroy(self, path):
        self.log.debug("destroy(%s)", path)
        self.table.refresh(wait_for_active=True)

    def truncate(self, path, length, fh=None):
        self.log.debug("truncate(%s, %d)", path, length)

        item = self.getRecordOrThrow(path)
        if not item.isFile():
            raise FuseOSError(EINVAL)

        item.truncate(length)

    def unlink(self, path):
        self.log.debug("unlink(%s)", path)

        self.getRecordOrThrow(path).delete()

    # TODO Should we instead implement MVCC?
    # TODO Or should we put big blocks onto S3
    # TODO Can we put the first block into the file item?
    # TODO Update modification time
    def write(self, path, data, offset, fh):
        self.log.debug("write(%s, len=%d, offset=%d)", path, len(data), offset)

        # TODO Cache opened item based on file handle
        # TODO What if item has changed underneath?

        item = self.getRecordOrThrow(path)
        if not item.isFile():
            raise FuseOSError(EINVAL)

        return item.write(data, offset)

    def read(self, path, size, offset, fh):
        self.log.debug("read(%s, size=%d, offset=%d)", path, size, offset)

        item = self.getRecordOrThrow(path)
        if not item.isFile():
            raise FuseOSError(EINVAL)

        return item.read(offset, size)

    def link(self, target, source):
        self.log.debug("link(%s, %s)", target, source)
        if len(target) > 1024:
            raise FuseOSError(ENAMETOOLONG)

        item = self.getRecordOrThrow(source)
        if not item.isFile():
            raise FuseOSError(EINVAL)

        if self.getItemOrNone(target, attrs=[]) is not None:
            raise FuseOSError(EEXIST)

        item.cloneItem(target)

    def lock(self, path, fip, cmd, lock):
        self.log.debug("lock(%s, fip=%x, cmd=%d, lock=(start=%d, len=%d, type=%x))", path, fip, cmd, lock.l_start, lock.l_len, lock.l_type)

        # Lock is optional if no concurrent access is expected
        # raise FuseOSError(EOPNOTSUPP)

    def bmap(self, path, blocksize, idx):
        self.log.debug("bmap(%s, blocksize=%d, idx=%d)", path, blocksize, idx)
        raise FuseOSError(EOPNOTSUPP)

    def mknod(self, path, mode, dev):
        self.log.debug("mknod(%s, mode=%d, dev=%d)", path, mode, dev)

        self.createRecord(path, "Node", attrs={'st_mode': mode, 'st_rdev': dev})
        return self.allocId()

        # ============ PRIVATE ====================

    def fileCount(self):
        self.table.refresh()
        return self.table.item_count

    def allocId(self):
        return self.counter.next()

    def checkFileExists(self, filepath):
        self.getItemOrThrow(filepath, attrs=[])

    def getItemOrThrow(self, filepath, attrs=None):
        if attrs is not None:
            if not "name" in attrs: attrs.append("name")
            if not "path" in attrs: attrs.append("path")
        name = os.path.basename(filepath)
        if name == "":
            name = "/"
        try:
            return self.table.get_item(os.path.dirname(filepath), name, attributes_to_get=attrs)
        except DynamoDBKeyNotFoundError:
            raise FuseOSError(ENOENT)

    def getItemOrNone(self, path, attrs=None):
        if attrs is not None:
            if not "name" in attrs: attrs.append("name")
            if not "path" in attrs: attrs.append("path")
        name = os.path.basename(path)
        if name == "":
            name = "/"
        try:
            return self.table.get_item(os.path.dirname(path), name, attributes_to_get=attrs)
        except DynamoDBKeyNotFoundError:
            return None

    def getRecordOrThrow(self, filepath, attrs=None):
        if attrs is not None:
            if not "name" in attrs: attrs.append("name")
            if not "path" in attrs: attrs.append("path")
            if not "type" in attrs: attrs.append("type")
        name = os.path.basename(filepath)
        if name == "":
            name = "/"
        try:
            return self.initRecord(filepath, self.table.get_item(os.path.dirname(filepath), name, attributes_to_get=attrs))
        except DynamoDBKeyNotFoundError:
            raise FuseOSError(ENOENT)

    def getRecordOrNone(self, path, attrs=None):
        if attrs is not None:
            if not "name" in attrs: attrs.append("name")
            if not "path" in attrs: attrs.append("path")
            if not "type" in attrs: attrs.append("type")
        name = os.path.basename(path)
        if name == "":
            name = "/"
        try:
            return self.initRecord(path, self.table.get_item(os.path.dirname(path), name, attributes_to_get=attrs))
        except DynamoDBKeyNotFoundError:
            return None


    def initRecord(self, path, item):
        record = getattr(getattr(dynamofuse.records, item['type'].lower()), item['type'])()
        record.init(self, path, item)
        return record

    def createRecord(self, path, type, attrs=None):
        record = getattr(getattr(dynamofuse.records, type.lower()), type)()
        record.create(self, path, attrs)
        return record

    def allocUniqueId(self):
        idItem = self.table.new_item(attrs={'name': 'counter', 'path': 'global'})
        idItem.add_attribute("value", 1)
        res = idItem.save(return_values="ALL_NEW")
        return res["Attributes"]["value"]

class BaseRecord:
    record = None
    accessor = None
    record = None

    def create(self, accessor, path, attrs):
        self.accessor = accessor
        self.path = path

        name = os.path.basename(path)
        if name == "":
            name = "/"
        l_time = int(time())
        newAttrs = {'name': name, 'path': os.path.dirname(path),
                    'type': self.__class__.__name__,
                    'st_nlink': 1,
                    'st_size': 0, 'st_ctime': l_time,
                    'st_mtime': l_time, 'st_atime': l_time
        }
        for k, v in attrs.items():
            newAttrs[k] = v
        item = self.accessor.table.new_item(attrs=attrs)
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
        attrs=dict((k, self.record[k]) for k in self.record.keys())
        attrs["path"] = os.path.dirname(path)
        attrs["name"] = os.path.basename(path)

        newItem = self.__class__()
        newItem.create(self.accessor, path, attrs)
        return newItem

    def getattr(self):
        return self.record

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

if __name__ == '__main__':
    if len(argv) != 4:
        print('usage: %s <region> <dynamo table> <mount point>' % argv[0])
        exit(1)

    logging.basicConfig(filename='/var/log/dynamo-fuse.log', filemode='w')
    logging.getLogger("dynamo-fuse").setLevel(logging.DEBUG)
    logging.getLogger("dynamo-fuse-file").setLevel(logging.DEBUG)
    logging.getLogger("fuse.log-mixin").setLevel(logging.INFO)
    logging.getLogger("dynamo-fuse-lock").setLevel(logging.DEBUG)
    logging.getLogger("dynamo-fuse-master").setLevel(logging.DEBUG)
    logging.getLogger("dynamo-fuse-block").setLevel(logging.DEBUG)

    fuse = FUSE(DynamoFS(argv[1], argv[2]), argv[3], foreground=True)


