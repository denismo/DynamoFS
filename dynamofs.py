#!/usr/bin/env python

# Copyright 2013 Denis Mikhalkin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import with_statement
import dynamofile

__author__ = 'Denis Mikhalkin'

from errno import *
from os.path import realpath
from sys import argv, exit
from threading import Lock
import boto.dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from boto.exception import BotoServerError, BotoClientError
from boto.exception import DynamoDBResponseError
from stat import S_IFDIR, S_IFLNK, S_IFREG, S_ISREG, S_ISDIR
from boto.dynamodb.types import Binary
from time import time
from boto.dynamodb.condition import EQ, GT
import os
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging
import sys
import cStringIO
import itertools

if not hasattr(__builtins__, 'bytes'):
    bytes = str

BLOCK_SIZE = 32768 # 64K minus 1K for path-name and about 1K for all other fields
ALL_ATTRS = None

class BotoExceptionMixin:
    log = logging.getLogger("dynamo-fuse")
    def __call__(self, op, path, *args):
        try:
            ret = getattr(self, op)(path, *args)
            self.log.debug("<- %s: %s", op, repr(ret))
            return ret
        except BotoServerError, e:
            self.log.error("<- %s: %s", op, repr(e))
            raise FuseOSError(EIO)
        except BotoClientError, e:
            self.log.error("<- %s: %s", op, repr(e))
            raise FuseOSError(EIO)
        except DynamoDBResponseError, e:
            self.log.error("<- %s: %s", op, repr(e))
            raise FuseOSError(EIO)

class DynamoFS(BotoExceptionMixin, Operations):
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

    def chmod(self, path, mode):
        self.log.debug("chmod(%s, mode=%d)", path, mode)
        item = self.getItemOrThrow(path, attrs=["st_mode"])
        item['st_mode'] &= 0770000
        item['st_mode'] |= mode
        item.save()
        return 0

    def chown(self, path, uid, gid):
        self.log.debug("chown(%s, uid=%d, gid=%d)", path, uid, gid)
        item = self.getItemOrThrow(path, attrs=["st_uid", "st_gid"])
        item['st_uid'] = uid
        item['st_gid'] = gid
        item.save()
        return 0

    def open(self, path, flags):
        self.log.debug("open(%s, flags=0x%x)", path, flags)
        # TODO read/write locking? Permission check?
        self.checkFileExists(path)
        return self.allocId()

    def utimens(self, path, times=None):
        self.log.debug("utimens(%s)", path)
        now = int(time())
        atime, mtime = times if times else (now, now)
        item = self.getItemOrThrow(path, attrs=["name", "path", "st_atime", "st_mtime"])
        item['st_atime'] = atime
        item['st_mtime'] = mtime
        item.save()

    def getattr(self, path, fh=None):
        self.log.debug("getattr(%s)", path)
        item = self.getItemOrThrow(path, attrs=None)
        if self.isFile(item):
            if not "st_blksize" in item:
                item["st_blksize"] = BLOCK_SIZE
            item["st_blocks"] = (item["st_size"] + item["st_blksize"]-1)/item["st_blksize"]
        return item

    def opendir(self, path):
        self.log.debug("opendir(%s)", path)
        self.checkFileExists(path)
        return self.allocId()

    def readdir(self, path, fh=None):
        self.log.debug("readdir(%s)", path)
        # Verify the directory exists
        self.checkFileDirExists(path)

        yield '.'
        yield '..'
        items = self.table.query(path, attributes_to_get=['name'])

        # TODO Pagination
        for entry in items:
            if entry['name'] == "/":
                continue # This could be the folder itself
            yield entry['name']

    def mkdir(self, path, mode):
        self.log.debug("mkdir(%s)", path)
        self.create(path, S_IFDIR | mode)

    # TODO Check if it is empty
    def rmdir(self, path):
        self.log.debug("rmdir(%s)", path)

        item = self.getItemOrThrow(path, attrs=['st_mode'])
        if not self.isDirectory(item):
            raise FuseOSError(EINVAL)

        item.delete()

    def rename(self, old, new):
        self.log.debug("rename(%s, %s)", old, new)
        if old == new: return
        if old == "/" or new == "/":
            raise FuseOSError(EINVAL)
        # TODO Check permissions in directories
        item = self.getItemOrThrow(old, attrs=ALL_ATTRS)
        newItem = self.getItemOrNone(new, attrs=["st_mode"])
        if not newItem is None:
            raise FuseOSError(EEXIST)
        else:
            attrsCopy={
                "path": os.path.dirname(new),
                "name": os.path.basename(new)
                }
            for k,v in item.items():
                if k == "name" or k == "path": continue
                attrsCopy[k] = v
            newItem = self.table.new_item(attrs=attrsCopy)
            newItem.put()

            if self.isDirectory(item):
                self.moveDirectory(old, new)

            item.delete()

    def readlink(self, path):
        self.log.debug("readlink(%s)", path)
        item = self.getItemOrThrow(path, attrs=['symlink'])
        if not "symlink" in item:
            raise FuseOSError(EINVAL)
        return item["symlink"]

    def symlink(self, target, source):
        self.log.debug("symlink(%s, %s)", target, source)
        if len(target) > 1024:
            raise FuseOSError(ENAMETOOLONG)
            # TODO: Verify does not exist
        # TODO: Update parent directory time
        name = os.path.basename(target)
        if name == "":
            name = "/"
        l_time = int(time())
        attrs = {'name': name, 'path': os.path.dirname(target),
                 'st_mode': S_IFLNK | 0777, 'st_nlink': 1,
                 'symlink': source, 'st_size': 0, 'st_ctime': l_time,
                 'st_mtime': l_time, 'st_atime': l_time
        }
        item = self.table.new_item(attrs=attrs)
        item.put()
        return 0

    def create(self, path, mode, fh=None):
        self.log.debug("create(%s, %d)", path, mode)
        if len(path) > 1024:
            raise FuseOSError(ENAMETOOLONG)
        # TODO: Verify does not exist
        # TODO: Update parent directory time
        l_time = int(time())
        name = os.path.basename(path)
        if name == "":
            name = "/"
        attrs = {'name': name, 'path': os.path.dirname(path),
                 'st_mode': mode, 'st_nlink': 1,
                 'st_size': 0, 'st_ctime': l_time, 'st_mtime': l_time,
                 'st_atime': l_time, 'st_blksize': BLOCK_SIZE}
        if mode & S_IFDIR == 0:
            mode |= S_IFREG
            attrs["st_mode"] = mode
            attrs["uniqueId"] = self.allocUniqueId()
        item = self.table.new_item(attrs=attrs)
        item.put()
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

        lastBlock = length / BLOCK_SIZE

        items = self.table.query(hash_key=path, range_key_condition=(GT(str(lastBlock)) if length else None), attributes_to_get=['name', "path"])
        # TODO Pagination
        for entry in items:
            entry.delete()

        # TODO Can optimize if length is stored as a field
        if length:
            lastItem = self.getItemOrNone(os.path.join(path, str(lastBlock)), attrs=["data", "name", "path"])
            if lastItem is not None and "data" in lastItem:
                lastItem['data'] = Binary(lastItem['data'].value[0:(length % BLOCK_SIZE)])
                lastItem.save()

        item = self.getItemOrThrow(path, attrs=['st_size', "name", "path"])
        item['st_size'] = length
        item.save()

    def unlink(self, path):
        self.log.debug("unlink(%s)", path)
        self.getItemOrThrow(path, attrs=['name', 'path']).delete()

        items = self.table.query(path, attributes_to_get=['name', 'path'])
        # TODO Pagination
        for entry in items:
            entry.delete()

    # TODO Should we instead implement MVCC?
    # TODO Or should we put big blocks onto S3
    # TODO Can we put the first block into the file item?
    # TODO Update modification time
    def write(self, path, data, offset, fh):
        self.log.debug("write(%s, len=%d, offset=%d)", path, len(data), offset)

        item = self.getItemOrThrow(path, attrs=["st_size", "uniqueId"])

        # TODO Cache opened item based on file handle
        file = dynamofile.DynamoFile(item, self)
        file.write(data, offset) # throws

        self.log.debug("write updating item st_size to %d", max(item["st_size"], offset + len(data)))
        item["st_size"] = max(item["st_size"], offset + len(data))
        item.save() # TODO What if item has changed underneath?

        return len(data)

    def read(self, path, size, offset, fh):
        self.log.debug("read(%s, size=%d, offset=%d)", path, size, offset)

        # TODO Cache opened item based on file handle
        file = dynamofile.DynamoFile(self.getItemOrThrow(path, attrs=["uniqueId"]), self)
        return file.read(offset, size) # throws

    def link(self, target, source):
        self.log.debug("link(%s, %s)", target, source)
        if len(target) > 1024:
            raise FuseOSError(ENAMETOOLONG)
        raise FuseOSError(EOPNOTSUPP)

    def lock(self, path, fip, cmd, lock):
        self.log.debug("lock(%s, fip=%x, cmd=%d, lock=(start=%d, len=%d, type=%x))", path, fip, cmd, lock.l_start, lock.l_len, lock.l_type)

        # Lock is optional if no concurrent access is expected
        # raise FuseOSError(EOPNOTSUPP)

    def bmap(self, path, blocksize, idx):
        self.log.debug("bmap(%s, blocksize=%d, idx=%d)", path, blocksize, idx)
        raise FuseOSError(EOPNOTSUPP)

        # ============ PRIVATE ====================

    def fileCount(self):
        self.table.refresh()
        return self.table.item_count

    def allocId(self):
#        idItem = self.table.new_item(attrs={'name': 'counter', 'path': 'global'})
#        idItem.add_attribute("value", 1)
#        res = idItem.save(return_values="ALL_NEW")
#        return res["Attributes"]["value"]
        return self.counter.next()

    def checkFileDirExists(self, filepath):
        self.checkFileExists(os.path.dirname(filepath))

    def checkFileExists(self, filepath):
        return self.getItemOrThrow(filepath, attrs=[])

    def getItemOrThrow(self, filepath, attrs=[]):
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

    def getItemOrNone(self, path, attrs=[]):
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

    def isFile(self, item):
        if item is not None:
            return S_ISREG(item["st_mode"])
        return False

    def isDirectory(self, item):
        if item is not None:
            return S_ISDIR(item["st_mode"])
        return False

    def isLink(self, item):
        if item is not None:
            return S_ISLNK(item["st_mode"])
        return False

    def newItem(self, attrs):
        return self.table.new_item(attrs=attrs)

    def allocUniqueId(self):
        idItem = self.table.new_item(attrs={'name': 'counter', 'path': 'global'})
        idItem.add_attribute("value", 1)
        res = idItem.save(return_values="ALL_NEW")
        return res["Attributes"]["value"]

    def moveDirectory(self, old, new):
        for entry in self.readdir(old):
            if entry == "." or entry == "..": continue;
            self.rename(os.path.join(old, entry), os.path.join(new, entry))

if __name__ == '__main__':
    if len(argv) != 4:
        print('usage: %s <region> <dynamo table> <mount point>' % argv[0])
        exit(1)

    logging.basicConfig(filename='/var/log/dynamo-fuse.log', filemode='w')
    logging.getLogger("dynamo-fuse").setLevel(logging.DEBUG)
    logging.getLogger("dynamo-fuse-file").setLevel(logging.DEBUG)
    logging.getLogger("fuse.log-mixin").setLevel(logging.INFO)
    logging.getLogger("dynamo-fuse-lock").setLevel(logging.DEBUG)

    fuse = FUSE(DynamoFS(argv[1], argv[2]), argv[3], foreground=True)


