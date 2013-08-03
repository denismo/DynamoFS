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

__author__ = 'Denis Mikhalkin'

import dynamofuse
from posix import R_OK, X_OK, W_OK
from dynamofuse.records.directory import Directory
from dynamofuse.records.file import File
from dynamofuse.records.node import Node
from dynamofuse.records.symlink import Symlink
from dynamofuse.base import BaseRecord, DELETED_LINKS, CONSISTENT_OPER
from dynamofuse.records.link import Link
from errno import *
from os.path import realpath
from sys import argv, exit
from threading import Lock
import boto.dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError, DynamoDBConditionalCheckFailedError
from boto.exception import BotoServerError, BotoClientError
from boto.exception import DynamoDBResponseError
from boto.dynamodb2.table import Table
from stat import *
from boto.dynamodb.types import Binary
from time import time, sleep, clock
from boto.dynamodb.condition import EQ, GT
import os
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn, fuse_get_context
from io import FileIO
import logging
from logging import StreamHandler, FileHandler
import sys
import cStringIO
import itertools
import traceback
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, AllIndex, IncludeIndex
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER, STRING

if not hasattr(__builtins__, 'bytes'):
    bytes = str

ALL_ATTRS = None
NAME_MAX = 255 # To match what is expected by Fuse and FSTest
KEY_MAX = 1024
global logStream

class BotoExceptionMixin:
    log = logging.getLogger("dynamo-fuse-oper  ")
    accessLog = logging.getLogger("dynamo-fuse-access")

    def __call__(self, op, path, *args):
        try:
            ret = getattr(self, op)(path, *args)
            self.log.debug("  - %s: %s", op, repr(ret))
            if logStream:
                logStream.flush()
            return ret
        except BotoServerError, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log.error("  - %s: %s", op, "".join(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            raise FuseOSError(EIO)
        except BotoClientError, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log.error("  - %s: %s", op, "".join(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            raise FuseOSError(EIO)
        except DynamoDBResponseError, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log.error("  - %s: %s", op, "".join(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            raise FuseOSError(EIO)
        except FuseOSError, e:
            self.log.error("  - %s: FuseOSError(%s)", op, e.strerror)
            raise e
        except BaseException, e:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            self.log.error("  - %s: %s", op, "".join(traceback.format_exception(exc_type, exc_value, exc_traceback)))
            raise FuseOSError(EIO)


class DynamoFS(BotoExceptionMixin, Operations):
    BLOCK_SIZE = 32768

    recordTypes = {
        "File": File,
        "Directory": Directory,
        "Symlink": Symlink,
        "Node": Node,
        "Link": Link
    }

    def __init__(self, uri):
        (unused, regionPath) = uri.split(':')
        (region, tableName) = regionPath.split('/')
        self.log = logging.getLogger("dynamo-fuse-oper  ")
        self.tableName = tableName
        self.region = region
        for reg in boto.dynamodb2.regions():
            if reg.name == region:
                self.regionv2 = reg
                break
        self.conn = boto.dynamodb.connect_to_region(region, aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
        connection = DynamoDBConnection(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'], region=self.regionv2)
        try:
            self.table = self.conn.get_table(tableName)
            self.tablev2 = Table(tableName, connection=connection)
            self.blockTable = self.conn.get_table(self.tableName + "Blocks")
            self.blockTablev2 = Table(self.tableName + "Blocks", connection=connection)
        except:
            self.createTable()
        self.counter = itertools.count()
        self.__createRoot()
        print "Ready"

    def createTable(self):
        connection = DynamoDBConnection(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'], region=self.regionv2)
        self.blockTablev2 = Table.create(self.tableName + "Blocks",
            schema=[
                HashKey('blockId'),
                RangeKey('blockNum', data_type=NUMBER)
            ],
            throughput={'read': 30, 'write': 10},
            connection=connection
        )
        self.tablev2 = Table.create(self.tableName,
            schema=[
                HashKey('path'),
                RangeKey('name')
            ],
            throughput={'read': 30, 'write': 10},
            indexes=[
                KeysOnlyIndex("Links", parts=[
                    HashKey('path'),
                    RangeKey('link')
                ])
            ],
            connection=connection
        )

        description = connection.describe_table(self.tableName)
        iter = 0
        while description["Table"]["TableStatus"] != "ACTIVE":
            print "Waiting for %s to create %d..." % (self.tableName, iter)
            iter += 1
            sleep(1)
            description = connection.describe_table(self.tableName)
        self.table = self.conn.get_table(self.tableName)
        self.blockTable = self.conn.get_table(self.tableName + "Blocks")

    def init(self, conn):
        self.log.debug(" init")

    def __createRoot(self):
        if not self.table.has_item("/", "/"):
            self.mkdir("/", 0755)
        if not self.table.has_item("/", DELETED_LINKS):
            self.mkdir("/" + DELETED_LINKS, 0755)

    def chmod(self, path, mode):
        self.log.debug(" chmod(%s, mode=%d)", path, mode)

        self.checkAccess(os.path.dirname(path), X_OK)

        self.getRecordOrThrow(path).chmod(mode)
        return 0

    def chown(self, path, uid, gid):
        self.log.debug(" chown(%s, uid=%d, gid=%d)", path, uid, gid)

        self.checkAccess(os.path.dirname(path), X_OK)

        self.getRecordOrThrow(path).chown(uid, gid)
        return 0

    def getattr(self, path, fh=None):
        self.log.debug(" getattr(%s)", path)

        self.checkAccess(os.path.dirname(path), X_OK)

        record = self.getRecordOrThrow(path)
        if record.isHidden():
            raise FuseOSError(ENOENT)

        return record.getattr()

    def open(self, path, flags):
        self.log.debug(" open(%s, flags=0x%x)", path, flags)

        access = X_OK
        if flags & os.O_CREAT: access |= W_OK
        self.checkAccess(os.path.dirname(path), access)
        self.checkFileExists(path)

        access = 0
        if flags & (os.O_RDONLY | os.O_RDWR) or flags == 0: access |= R_OK
        if flags & (os.O_WRONLY | os.O_RDWR | os.O_APPEND | os.O_TRUNC): access |= W_OK

        self.checkAccess(path, access)

        return self.allocId()

    def utimens(self, path, times=None):
        self.log.debug(" utimens(%s)", path)
        now = int(time())
        atime, mtime = times if times else (now, now)

        item = self.getRecordOrThrow(path)

        item.utimens(atime, mtime)

    def opendir(self, path):
        self.log.debug(" opendir(%s)", path)

        self.checkAccess(os.path.dirname(path), X_OK)
        self.checkFileExists(path)
        self.checkAccess(path, R_OK | X_OK)

        return self.allocId()

    def readdir(self, path, fh=None):
        self.log.debug(" readdir(%s)", path)
        # Verify the directory exists
        dir = self.getRecordOrThrow(path)

        if dir.access(R_OK | X_OK):
            raise FuseOSError(EACCES)

        yield '.'
        yield '..'
        for v in dir.list(): yield v

    def mkdir(self, path, mode):
        self.log.debug(" mkdir(%s)", path)

        if path != "/":
            self.checkAccess(os.path.dirname(path), R_OK | W_OK | X_OK)

        self.create(path, mode | S_IFDIR)

    def rmdir(self, path):
        self.log.debug(" rmdir(%s)", path)

        item = self.getRecordOrThrow(path)

        if not item.isDirectory():
            raise FuseOSError(EINVAL)

        self.checkAccess(os.path.dirname(path), R_OK | W_OK | X_OK)
        self.checkSticky(path)

        if len(list(item.list())) > 0:
            raise FuseOSError(ENOTEMPTY)

        item.delete()

    def rename(self, old, new):
        self.log.debug(" rename(%s, %s)", old, new)
        if old == new: return
        if old == "/" or new == "/":
            raise FuseOSError(EINVAL)

        self.checkPath(new)
        self.checkAccess(os.path.dirname(old), R_OK | W_OK | X_OK)
        self.checkAccess(os.path.dirname(new), R_OK | W_OK | X_OK)
        self.checkSticky(old, new)

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
        self.log.debug(" readlink(%s)", path)

        item = self.getRecordOrThrow(path)

        if not item.isLink():
            raise FuseOSError(EINVAL)

        return item["symlink"]

    def symlink(self, target, source):
        self.log.debug(" symlink(%s, %s)", target, source)

        self.checkAccess(os.path.dirname(target), R_OK | W_OK | X_OK)

        try:
            record = self.createRecord(target, "Symlink", attrs={'symlink': source})
        except DynamoDBConditionalCheckFailedError: # Means the item already exists
            raise FuseOSError(EEXIST)

        record.updateDirectoryMCTime(target)

        return 0

    def create(self, path, mode, fh=None):
        self.log.debug(" create(%s, %d)", path, mode)

        if path != "/":
            self.checkAccess(os.path.dirname(path), R_OK | X_OK | W_OK)

        type = "Node"
        if mode & S_IFDIR == S_IFDIR:
            type = "Directory"
        elif mode & S_IFLNK == S_IFLNK:
            type = "Symlink"
        elif mode & S_IFIFO == S_IFIFO or mode & S_IFBLK == S_IFBLK or mode & S_IFCHR == S_IFCHR or mode & S_IFSOCK == S_IFSOCK:
            type = "Node"
        elif mode & S_IFREG == S_IFREG:
            type = "File"

        attrs = {'st_mode': mode}
        if os.path.basename(path) == DELETED_LINKS:
            attrs['hidden'] = True

        try:
            record = self.createRecord(path, type, attrs=attrs)
        except DynamoDBConditionalCheckFailedError: # Means the item already exists
            raise FuseOSError(EEXIST)

        # Update
        if path != "/":
            record.updateDirectoryMCTime(path)

        return self.allocId()

    def statfs(self, path):
        self.log.debug(" statfs(%s)", path)
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
        self.log.debug(" destroy(%s)", path)
        self.table.refresh(wait_for_active=True)

    def truncate(self, path, length, fh=None):
        self.log.debug(" truncate(%s, %d)", path, length)

        item = self.getRecordOrThrow(path)
        if not item.isFile():
            raise FuseOSError(EINVAL)

        self.checkAccess(os.path.dirname(path), X_OK)

        if item.access(W_OK):
            raise FuseOSError(EACCES)

        item.truncate(length)

    def unlink(self, path):
        self.log.debug(" unlink(%s)", path)

        self.checkAccess(os.path.dirname(path), W_OK | X_OK)
        self.checkSticky(path)

        self.getRecordOrThrow(path).delete()

    def write(self, path, data, offset, fh):
        self.log.debug(" write(%s, len=%d, offset=%d)", path, len(data), offset)

        item = self.getRecordOrThrow(path)
        if not item.isFile() and not item.isHardLink():
            raise FuseOSError(EINVAL)

        return item.write(data, offset)

    def read(self, path, size, offset, fh):
        self.log.debug(" read(%s, size=%d, offset=%d)", path, size, offset)

        item = self.getRecordOrThrow(path)
        if not item.isFile() and not item.isHardLink():
            raise FuseOSError(EINVAL)

        return item.read(offset, size)

    def link(self, target, source):
        self.log.debug(" link(%s, %s)", target, source)

        self.checkAccess(source, R_OK)

        item = self.getRecordOrThrow(source)
        if not item.isFile() and not item.isNode() and not item.isHardLink():
            raise FuseOSError(EINVAL)

        if item.isHardLink():
            item = item.getLink()

        self.checkAccess(os.path.dirname(target), R_OK | W_OK | X_OK)
        self.checkAccess(os.path.dirname(source), R_OK | X_OK)

        try:
            record = Link()
            record.createRecord(self, target, {}, item)
        except DynamoDBConditionalCheckFailedError: # Means the item already exists
            raise FuseOSError(EEXIST)

        item.updateDirectoryMCTime(source)
        record.updateDirectoryMCTime(target)

        return 0

    def lock(self, path, fip, cmd, lock):
        self.log.debug(" lock(%s, fip=%x, cmd=%d, lock=(start=%d, len=%d, type=%x))", path, fip, cmd, lock.l_start,
            lock.l_len, lock.l_type)
        return 0
        # Lock is optional if no concurrent access is expected
        # raise FuseOSError(EOPNOTSUPP)

    def bmap(self, path, blocksize, idx):
        self.log.debug(" bmap(%s, blocksize=%d, idx=%d)", path, blocksize, idx)
        raise FuseOSError(EOPNOTSUPP)

    def mknod(self, path, mode, dev):
        self.log.debug(" mknod(%s, mode=%d, dev=%d)", path, mode, dev)

        self.checkAccess(os.path.dirname(path), R_OK | W_OK | X_OK)

        try:
            record = self.createRecord(path, "File", attrs={'st_mode': mode, 'st_rdev': dev})
        except DynamoDBConditionalCheckFailedError: # Means the item already exists
            raise FuseOSError(EEXIST)

        # Update
        if path != "/":
            record.updateDirectoryMCTime(path)

        return 0

    def access(self, path, amode):
        (uid, gid, unused) = fuse_get_context()
        self.accessLog.debug(" access(%s, mode=%d) by (%d, %d)", path, amode, uid, gid)
        item = self.getRecordOrThrow(path)
        return item.access(amode)

        # ============ PRIVATE ====================

    def checkSticky(self, old, new=None):
        (uid, gid, unused) = fuse_get_context()
        oldDir = self.getRecordOrThrow(os.path.dirname(old))

        if oldDir.isSticky():
            oldItem = self.getRecordOrThrow(old)
            if uid != 0 and not (uid == oldDir.getOwner() or uid == oldItem.getOwner()):
                raise FuseOSError(EPERM)

        if new:
            newItem = self.getRecordOrNone(new)
            if newItem:
                parent = newItem.getParent(self)
                if parent.isSticky():
                    if uid:
                        if not (newItem.getOwner() == uid or parent.getOwner() == uid):
                            raise FuseOSError(EPERM)

    def checkAccess(self, path, mode):
        if not self.access(path, mode) == 0:
            raise FuseOSError(EACCES)

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

    def absPath(self, file, refDir):
        if file.startsWith('/'):
            return file
        return os.path.join(refDir, file)

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
            return self.table.get_item(os.path.dirname(filepath), name, attributes_to_get=attrs, consistent_read=CONSISTENT_OPER)
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
            return self.table.get_item(os.path.dirname(path), name, attributes_to_get=attrs, consistent_read=CONSISTENT_OPER)
        except DynamoDBKeyNotFoundError:
            return None

    def getRecordOrThrow(self, filepath, attrs=None, ignoreDeleted=False):
        self.checkPath(filepath)
        if attrs is not None:
            for i in ["name", "path", "type", "version"]:
                if not i in attrs: attrs.append(i)
        name = os.path.basename(filepath)
        if name == "":
            name = "/"
        try:
            res = self.initRecord(filepath,
                self.table.get_item(os.path.dirname(filepath), name, attributes_to_get=attrs, consistent_read=CONSISTENT_OPER))
            if not ignoreDeleted and res.isDeleted():
                raise FuseOSError(ENOENT)
            return res
        except DynamoDBKeyNotFoundError:
            raise FuseOSError(ENOENT)

    def getRecordOrNone(self, path, attrs=None, ignoreDeleted=False):
        self.checkPath(path)
        if attrs is not None:
            for i in ["name", "path", "type", "version"]:
                if not i in attrs: attrs.append(i)
        name = os.path.basename(path)
        if name == "":
            name = "/"
        try:
            res = self.initRecord(path,
                self.table.get_item(os.path.dirname(path), name, attributes_to_get=attrs, consistent_read=CONSISTENT_OPER))
            if not ignoreDeleted and res.isDeleted():
                raise FuseOSError(ENOENT)
            return res
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


def cleanup(uri):
    (unused, regionPath) = uri.split(':')
    (region, tableName) = regionPath.split('/')
    conn = boto.dynamodb.connect_to_region(region, aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    table = conn.get_table(tableName)
    for item in table.scan(attributes_to_get=["name", "path"]):
        if item["path"] == "/" and item["name"] == "/": continue
        if item["path"] == "global" and item["name"] == "counter": continue
        if item["path"] == '/' and item['name'] == DELETED_LINKS: continue
        item.delete()

if __name__ == '__main__':
    if len(argv) != 3:
        print('usage: %s aws:<region>/<dynamo table> <mount point>' % argv[0])
        exit(1)

    logStream = open('/var/log/dynamo-fuse.log', 'w', 0)
    logging.basicConfig(stream=logStream)
    logging.getLogger("dynamo-fuse-oper  ").setLevel(logging.DEBUG)
    logging.getLogger("dynamo-fuse-access").setLevel(logging.INFO)
    logging.getLogger("dynamo-fuse-record").setLevel(logging.INFO)
    logging.getLogger("dynamo-fuse-file  ").setLevel(logging.DEBUG)
    logging.getLogger("fuse.log-mixin").setLevel(logging.INFO)
    logging.getLogger("dynamo-fuse-lock  ").setLevel(logging.DEBUG)
    logging.getLogger("dynamo-fuse-master").setLevel(logging.DEBUG)
    logging.getLogger("dynamo-fuse-block ").setLevel(logging.DEBUG)

    if argv[2] == "cleanup":
        cleanup(argv[1])
    elif argv[2] == "createTable":
        DynamoFS(argv[1]).createTable()
    else:
        fuse = FUSE(DynamoFS(argv[1]), argv[2], foreground=True, nothreads=True, default_permissions=False,
            auto_cache=False,
            noauto_cache=True, kernel_cache=False, direct_io=True, allow_other=True, use_ino=True, attr_timeout=0)


