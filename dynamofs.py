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

from errno import EACCES, ENOENT, EINVAL, EEXIST, EOPNOTSUPP, EIO
from os.path import realpath
from sys import argv, exit
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

BLOCK_SIZE = 63000 # 64K minus 1K for path-name and about 1K for all other fields

class DynamoFS(Operations):
    def __init__(self, region, tableName):
        self.log = logging.getLogger("dynamo-fuse")
        self.tableName = tableName
        self.conn = boto.dynamodb.connect_to_region(region, aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
        self.table = self.conn.get_table(tableName)
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

    def open(self, path, flags):
        self.log.debug("open(%s, flags=%x)", path, flags)
        # TODO read/write locking? Permission check?
        self.checkFileExists(path)
        return self.allocId()

    def utimens(self, path, times=None):
        self.log.debug("utimens(%s)", path)
        now = int(time())
        atime, mtime = times if times else (now, now)
        try:
            item = self.table.get_item(os.path.dirname(path), name)
        except DynamoDBKeyNotFoundError:
            raise FuseOSError(ENOENT)
        else:
            item['st_atime'] = atime
            item['st_mtime'] = mtime
            item.save()

    def getattr(self, path, fh=None):
        self.log.debug("getattr(%s)", path)
        return self.checkFileExists(path)

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
        if (item["st_mode"] & S_IFDIR) != S_IFDIR:
            raise FuseOSError(EINVAL)

        item.delete()

    def rename(self, old, new):
        self.log.debug("rename(%s, %s)", old, new)
        if old == new: return
        # TODO Check permissions in directories
        item = self.getItemOrThrow(old)
        newItem = self.getItemOrNone(new, attrs=["st_mode"])
        if self.isDirectory(newItem):
            item.hash_key = new
            item.save()
        elif self.isFile(newItem):
            raise FuseOSError(EEXIST)
        else:
            item.hash_key = os.path.dirname(new)
            item.range_key = os.path.basename(new)
            item.save()

    def readlink(self, path):
        self.log.debug("readlink(%s)", path)
        item = self.getItemOrThrow(path, attrs=['symlink'])
        if not "symlink" in item:
            raise FuseOSError(EINVAL)
        return item["symlink"]

    def symlink(self, target, source):
        self.log.debug("symlink(%s, %s)", target, source)
        # TODO: Verify does not exist
        # TODO: Update parent directory time
        name = os.path.basename(target)
        if name == "":
            name = "/"
        attrs = {'name': name, 'path': os.path.dirname(target),
                 'st_mode': S_IFLNK | 0777, 'st_nlink': 1,
                 'symlink': source
        }
        item = self.table.new_item(attrs=attrs)
        item.put()
        return self.allocId()

    def create(self, path, mode, fh=None):
        self.log.debug("create(%s, %d)", path, mode)
        # TODO: Verify does not exist
        # TODO: Update parent directory time
        l_time = int(time())
        name = os.path.basename(path)
        if name == "":
            name = "/"
        attrs = {'name': name, 'path': os.path.dirname(path),
                 'st_mode': mode, 'st_nlink': 1,
                 'st_size': 0, 'st_ctime': l_time, 'st_mtime': l_time,
                 'st_atime': l_time}
        if mode & S_IFDIR == 0:
            mode |= S_IFREG
            attrs["mode"] = mode
        item = self.table.new_item(attrs=attrs)
        item.put()
        return self.allocId()

    def statfs(self, path):
        self.log.debug("statfs(%s)", path)
        return dict(
            f_bsize=BLOCK_SIZE,
            f_frsize=BLOCK_SIZE,
            f_blocks=(sys.maxint - 1) / BLOCK_SIZE,
            f_bfree=(sys.maxint - 1) / BLOCK_SIZE,
            f_bavail=(sys.maxint - 1) / BLOCK_SIZE,
            f_files=self.fileCount(),
            f_ffree=sys.maxint - 1,
            f_favail=sys.maxint - 1,
            #            f_fsid=1023,
            #            f_flag=1022,
            #            f_namemax=1024,
            #            f_blocks=1024*1024,
            #            f_bfree= 1024*1024,
            #            f_bavail=1024*1024,
            #            f_files= 1,
            #            f_ffree= 1024*1024,
            #            f_favail=1024*1024,
            f_fsid=1,
            f_flag=0,
            f_namemax=1024
        )

    def destroy(self, path):
        self.log.debug("destroy(%s)", path)
        self.table.refresh(wait_for_active=True)

    def truncate(self, path, length, fh=None):
        self.log.debug("truncate(%s, %d)", path, length)

        lastBlock = length / BLOCK_SIZE

        items = self.table.query(hash_key=path, range_key_condition=GT(str(lastBlock)), attributes_to_get=['name'])
        # TODO Pagination
        for entry in items:
            entry.delete()

        # TODO Can optimize if length is stored as a field
        lastItem = self.getItemOrNone(os.path.join(path, str(lastBlock)), attrs=["data"])
        if lastItem is not None and "data" in lastItem:
            lastItem['data'] = lastItem['data'][0:(length % BLOCK_SIZE)]
            lastItem.save()

        item = self.getItemOrThrow(path, attrs=['st_size'])
        item['st_size'] = length
        item.save()

    def unlink(self, path):
        self.log.debug("unlink(%s)", path)
        self.getItemOrThrow(path, attrs=['name']).delete()

        items = self.table.query(path, attributes_to_get=['name'])
        # TODO Pagination
        for entry in items:
            entry.delete()

    # TODO Should we instead implement MVCC?
    # TODO Or should we put big blocks onto S3
    # TODO Can we put the first block into the file item?
    # TODO Update modification time
    def write(self, path, data, offset, fh):
        self.log.debug("write(%s, len=%d, offset=%d)", path, len(data), offset)

        file = dynamofile.DynamoFile(path, self)
        file.write(data, offset) # throws

        item = self.getItemOrThrow(path, attrs=["st_size"])
        self.log.debug("write updating item st_size to %d", max(item["st_size"], offset + len(data)))
        item["st_size"] = max(item["st_size"], offset + len(data))
        item.save()

        return len(data)

    def read(self, path, size, offset, fh):
        self.log.debug("read(%s, size=%d, offset=%d)", path, size, offset)

        file = dynamofile.DynamoFile(path, self)
        return file.read(offset, size) # throws

    def link(self, target, source):
        self.log.debug("link(%s, %s)", target, source)
        raise FuseOSError(EOPNOTSUPP)

    def lock(self, path, fip, cmd, lock):
        self.log.debug("lock(%s, fip=%x, cmd=%x, lock=%x)", path, fip, cmd, lock)
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
        idItem = self.table.new_item(attrs={'name': 'counter', 'path': 'global'})
        idItem.add_attribute("value", 1)
        res = idItem.save(return_values="ALL_NEW")
        return res["Attributes"]["value"]

    def checkFileDirExists(self, filepath):
        self.checkFileExists(os.path.dirname(filepath))

    def checkFileExists(self, filepath):
        return self.getItemOrThrow(filepath, attrs=[])

    def getItemOrThrow(self, filepath, attrs=None):
        name = os.path.basename(filepath)
        if attrs is None:
            attrs = ['name']
        if name == "":
            name = "/"
        try:
            return self.table.get_item(os.path.dirname(filepath), name, attributes_to_get=attrs)
        except DynamoDBKeyNotFoundError:
            raise FuseOSError(ENOENT)

    def getItemOrNone(self, path, attrs=["name"]):
        name = os.path.basename(path)
        if name == "":
            name = "/"
        try:
            return self.table.get_item(os.path.dirname(path), name, attributes_to_get=attrs)
        except DynamoDBKeyNotFoundError:
            return None

    def isFile(self, item):
        if item is not None:
            return (item["st_mode"] & S_IFREG) == S_IFREG

    def isDirectory(self, item):
        if item is not None:
            return (item["st_mode"] & S_IFDIR) == S_IFDIR

    def newItem(self, attrs):
        return self.table.new_item(attrs=attrs)

if __name__ == '__main__':
    if len(argv) != 4:
        print('usage: %s <region> <dynamo table> <mount point>' % argv[0])
        exit(1)

    logging.basicConfig(filename='/var/log/dynamo-fuse.log', filemode='w')
    logging.getLogger("dynamo-fuse").setLevel(logging.DEBUG)
    logging.getLogger("dynamo-fuse-file").setLevel(logging.DEBUG)
    logging.getLogger("fuse.log-mixin").setLevel(logging.INFO)

    fuse = FUSE(DynamoFS(argv[1], argv[2]), argv[3], foreground=True)
