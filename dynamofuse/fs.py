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
from dynamofuse.base import BaseRecord

__author__ = 'Denis Mikhalkin'

import dynamofuse
from dynamofuse.records.directory import Directory
from dynamofuse.records.file import File
from dynamofuse.records.node import Node
from dynamofuse.records.symlink import Symlink
from errno import *
from os.path import realpath
from sys import argv, exit
from threading import Lock
import boto.dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from boto.exception import BotoServerError, BotoClientError
from boto.exception import DynamoDBResponseError
from stat import S_IFDIR, S_IFLNK, S_IFREG, S_ISREG, S_ISDIR, S_ISLNK, S_IFIFO, S_IFBLK, S_IFCHR, S_IFSOCK
from boto.dynamodb.types import Binary
from time import time
from boto.dynamodb.condition import EQ, GT
import os
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from io import FileIO
import logging
from logging import StreamHandler, FileHandler
import sys
import cStringIO
import itertools
import traceback

if not hasattr(__builtins__, 'bytes'):
    bytes = str

ALL_ATTRS = None
NAME_MAX=255 # To match what is expected by Fuse and FSTest
KEY_MAX=1024
global logStream

class BotoExceptionMixin:
    log = logging.getLogger("dynamo-fuse")
    def __call__(self, op, path, *args):
        try:
            ret = getattr(self, op)(path, *args)
            self.log.debug("<- %s: %s", op, repr(ret))
            if logStream:
                logStream.flush()
            return ret
        except BotoServerError, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log.error("<- %s: %s", op, "".join(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            raise FuseOSError(EIO)
        except BotoClientError, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log.error("<- %s: %s", op, "".join(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            raise FuseOSError(EIO)
        except DynamoDBResponseError, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log.error("<- %s: %s", op, "".join(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            raise FuseOSError(EIO)
        except FuseOSError, e:
            self.log.error("<- %s: FuseOSError(%s)", op, e.strerror)
            raise e
        except BaseException, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log.error("<- %s: %s", op, "".join(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            raise FuseOSError(EIO)


class DynamoFS(BotoExceptionMixin, Operations):
    BLOCK_SIZE = 32768

    recordTypes = {
        "File": File,
        "Directory": Directory,
        "Symlink": Symlink,
        "Node": Node
    }

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
#    def access(self, path, amode):
#        self.log.debug("access(%s, %x)", path, amode)
#
#        item = self.getRecordOrThrow(path)
#        return item.access(amode)

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

    def utimens(self, path, times=None):
        self.log.debug("utimens(%s)", path)
        now = int(time())
        atime, mtime = times if times else (now, now)

        item = self.getRecordOrThrow(path)

        item.utimens(atime, mtime)

    def opendir(self, path):
        self.log.debug("opendir(%s)", path)
        self.checkFileExists(path)
        return self.allocId()

    def readdir(self, path, fh=None):
        self.log.debug("readdir(%s)", path)
        # Verify the directory exists
        dir = self.getRecordOrThrow(path)

        yield '.'
        yield '..'
        for v in dir.list(): yield v

    def mkdir(self, path, mode):
        self.log.debug("mkdir(%s)", path)
        self.create(path, mode | S_IFDIR)

    def rmdir(self, path):
        self.log.debug("rmdir(%s)", path)

        item = self.getRecordOrThrow(path)

        if not item.isDirectory():
            raise FuseOSError(EINVAL)

        if len(list(item.list())) > 0:
            raise FuseOSError(ENOTEMPTY)

        item.delete()

    def rename(self, old, new):
        '''
        oldpath can specify a directory. In this case, newpath must either not exist, or it must specify an empty directory.

However, when overwriting there will probably be a window in which both oldpath and newpath refer to the file being renamed.

If oldpath refers to a symbolic link the link is renamed; if newpath refers to a symbolic link the link will be overwritten.

EINVAL
The new pathname contained a path prefix of the old, or, more generally, an attempt was made to make a directory a subdirectory of itself.

EISDIR
newpath is an existing directory, but oldpath is not a directory.

ENOENT
The link named by oldpath does not exist; or, a directory component in newpath does not exist; or, oldpath or newpath is an empty string.

ENOTDIR
A component used as a directory in oldpath or newpath is not, in fact, a directory. Or, oldpath is a directory, and newpath exists but is not a directory.

ENOTEMPTY or EEXIST
newpath is a nonempty directory, that is, contains entries other than "." and "..".
        '''
        self.log.debug("rename(%s, %s)", old, new)
        if old == new: return
        if old == "/" or new == "/":
            raise FuseOSError(EINVAL)

        self.checkPath(new)

        item = self.getRecordOrThrow(old)
        newItem = self.getRecordOrNone(new)
        if item.isDirectory():
            if not newItem is None:
                if not newItem.isDirectory():
                    raise FuseOSError(EISDIR)
                if not newItem.isEmpty():
                    raise FuseOSError(ENOTEMPTY)

        newDir = self.getItemOrNone(os.path.dirname(new), attrs=['type'])
        if newDir is None or not ('type' in newDir and newDir["type"] == "Directory"):
            raise FuseOSError(ENOENT)

        item.moveTo(new)

    def readlink(self, path):
        self.log.debug("readlink(%s)", path)

        item = self.getRecordOrThrow(path)

        if not item.isLink():
            raise FuseOSError(EINVAL)

        return item["symlink"]

    def symlink(self, target, source):
        self.log.debug("symlink(%s, %s)", target, source)

        # getItemOrNone will check path
        item = self.getItemOrNone(target, attrs=[])
        if item is not None:
            raise FuseOSError(EEXIST)

        record = self.createRecord(target, "Symlink", attrs={'symlink': source})
        record.updateDirectoryMCTime(target)

        return 0

    def create(self, path, mode, fh=None):
        self.log.debug("create(%s, %d)", path, mode)

        # getItemOrNone will check path
        item = self.getItemOrNone(path, attrs=[])
        if item is not None:
            raise FuseOSError(EEXIST)

        # TODO: Update parent directory time
        type = "Node"
        if mode & S_IFDIR == S_IFDIR:
            type = "Directory"
        elif mode & S_IFLNK == S_IFLNK:
            type = "Symlink"
        elif mode & S_IFIFO == S_IFIFO or mode & S_IFBLK == S_IFBLK or mode & S_IFCHR == S_IFCHR or mode & S_IFSOCK == S_IFSOCK:
            type = "Node"
        elif mode & S_IFREG == S_IFREG:
            type = "File"

        self.createRecord(path, type, attrs={'st_mode': mode})

        # Update
        if path != "/":
            dir = self.getRecordOrThrow(os.path.dirname(path))
            dir.updateMCTime()

        return self.allocId()

    def statfs(self, path):
        self.log.debug("statfs(%s)", path)
        return dict(
            f_bsize=self.BLOCK_SIZE,
            f_frsize=self.BLOCK_SIZE,
            f_blocks=(sys.maxint - 1),
            f_bfree=(sys.maxint - 2),
            f_bavail=(sys.maxint - 2),
            f_files=self.fileCount(),
            f_ffree=sys.maxint - 1,
            f_favail=sys.maxint - 1,
            f_fsid=0,
            f_flag=0,
            f_namemax=NAME_MAX
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

        item = self.getRecordOrThrow(source)
        if not item.isFile() and not item.isNode():
            raise FuseOSError(EINVAL)

        if self.getItemOrNone(target, attrs=[]) is not None:
            raise FuseOSError(EEXIST)

        item.cloneItem(target)

        sourceDir = self.getRecordOrThrow(os.path.dirname(source))
        sourceDir.updateCTime()

        return 0

    def lock(self, path, fip, cmd, lock):
        self.log.debug("lock(%s, fip=%x, cmd=%d, lock=(start=%d, len=%d, type=%x))", path, fip, cmd, lock.l_start, lock.l_len, lock.l_type)
        return 0
        # Lock is optional if no concurrent access is expected
        # raise FuseOSError(EOPNOTSUPP)

    def bmap(self, path, blocksize, idx):
        self.log.debug("bmap(%s, blocksize=%d, idx=%d)", path, blocksize, idx)
        raise FuseOSError(EOPNOTSUPP)

    def mknod(self, path, mode, dev):
        self.log.debug("mknod(%s, mode=%d, dev=%d)", path, mode, dev)

        self.createRecord(path, "Node", attrs={'st_mode': mode, 'st_rdev': dev})
        return 0

        # ============ PRIVATE ====================

    def checkPath(self, path):
        if len(path) > 4096:
            raise FuseOSError(ENAMETOOLONG)

        name = os.path.basename(path)
        if name == "":
            if os.path.dirname(path) != "/":
                raise FuseOSError(EINVAL)
            name = "/"
        if len(name) > min(NAME_MAX, KEY_MAX):
            raise FuseOSError(ENAMETOOLONG)
        if len(os.path.dirname(path)) > KEY_MAX:
            raise FuseOSError(ENAMETOOLONG)

    def fileCount(self):
        self.table.refresh()
        return self.table.item_count

    def allocId(self):
        return self.counter.next()

    def checkFileExists(self, filepath):
        self.getItemOrThrow(filepath, attrs=[])

    def newItem(self, attrs):
        return self.table.new_item(attrs=attrs)

    def getItemOrThrow(self, filepath, attrs=None):
        self.checkPath(filepath)
        if attrs is not None:
            if not "name" in attrs: attrs.append("name")
            if not "path" in attrs: attrs.append("path")
        name = os.path.basename(filepath)
        if name == "":
            name = "/"
        try:
            return self.table.get_item(os.path.dirname(filepath), name, attributes_to_get=attrs, consistent_read=True)
        except DynamoDBKeyNotFoundError:
            raise FuseOSError(ENOENT)

    def getItemOrNone(self, path, attrs=None):
        self.checkPath(path)
        if attrs is not None:
            if not "name" in attrs: attrs.append("name")
            if not "path" in attrs: attrs.append("path")
        name = os.path.basename(path)
        if name == "":
            name = "/"
        try:
            return self.table.get_item(os.path.dirname(path), name, attributes_to_get=attrs, consistent_read=True)
        except DynamoDBKeyNotFoundError:
            return None

    def getRecordOrThrow(self, filepath, attrs=None):
        self.checkPath(filepath)
        if attrs is not None:
            if not "name" in attrs: attrs.append("name")
            if not "path" in attrs: attrs.append("path")
            if not "type" in attrs: attrs.append("type")
        name = os.path.basename(filepath)
        if name == "":
            name = "/"
        try:
            return self.initRecord(filepath, self.table.get_item(os.path.dirname(filepath), name, attributes_to_get=attrs, consistent_read=True))
        except DynamoDBKeyNotFoundError:
            raise FuseOSError(ENOENT)

    def getRecordOrNone(self, path, attrs=None):
        self.checkPath(path)
        if attrs is not None:
            if not "name" in attrs: attrs.append("name")
            if not "path" in attrs: attrs.append("path")
            if not "type" in attrs: attrs.append("type")
        name = os.path.basename(path)
        if name == "":
            name = "/"
        try:
            return self.initRecord(path, self.table.get_item(os.path.dirname(path), name, attributes_to_get=attrs, consistent_read=True))
        except DynamoDBKeyNotFoundError:
            return None

    def initRecord(self, path, item):
        record = self.recordTypes[item['type']]()
        record.init(self, path, item)
        return record

    def createRecord(self, path, type, attrs=None):
        record = self.recordTypes[type]()
        record.create(self, path, attrs)
        return record

    def allocUniqueId(self):
        idItem = self.table.new_item(attrs={'name': 'counter', 'path': 'global'})
        idItem.add_attribute("value", 1)
        res = idItem.save(return_values="ALL_NEW")
        return res["Attributes"]["value"]

if __name__ == '__main__':
    if len(argv) != 4:
        print('usage: %s <region> <dynamo table> <mount point>' % argv[0])
        exit(1)

#    logging.basicConfig(filename='/var/log/dynamo-fuse.log', filemode='w')
    logStream = open('/var/log/dynamo-fuse.log', 'w', 0)
    logging.basicConfig(stream=logStream)
    logging.getLogger("dynamo-fuse").setLevel(logging.DEBUG)
    logging.getLogger("dynamo-fuse-file").setLevel(logging.DEBUG)
    logging.getLogger("fuse.log-mixin").setLevel(logging.INFO)
    logging.getLogger("dynamo-fuse-lock").setLevel(logging.DEBUG)
    logging.getLogger("dynamo-fuse-master").setLevel(logging.DEBUG)
    logging.getLogger("dynamo-fuse-block").setLevel(logging.DEBUG)

    fuse = FUSE(DynamoFS(argv[1], argv[2]), argv[3], foreground=True, nothreads=True, default_permissions=True, kernel_cache=False, direct_io=True, allow_other=True, use_ino=True)


