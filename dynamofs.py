#!/usr/bin/env python

from __future__ import with_statement

__author__ = 'Denis Mikhalkin'

from errno import EACCES,ENOENT,EINVAL,EEXIST
from os.path import realpath
from sys import argv, exit
from threading import Lock
import boto.dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time
from boto.dynamodb.condition import EQ
import os
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class DynamoFS(LoggingMixIn, Operations):
    def __init__(self, region, tableName):
        self.tableName = tableName
        self.conn = boto.dynamodb.connect_to_region(region, aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
        self.table = self.conn.get_table(tableName)
        self.__createRoot()

    def __createRoot(self):
        if not self.table.has_item("/", "/"):
            self.mkdir("/", 0755)

    def chmod(self, path, mode):
        try:
            self.table.get_item(os.path.dirname(path), name, attributes_to_get=['name'])
        except DynamoDBKeyNotFoundError:
            raise FuseOSError(ENOENT)
        else:
            item['st_mode'] &= 0770000
            item['st_mode'] |= mode
            item.update()
            return 0

    def chown(self, path, uid, gid):
        try:
            self.table.get_item(os.path.dirname(path), name, attributes_to_get=['name'])
        except DynamoDBKeyNotFoundError:
            raise FuseOSError(ENOENT)
        else:
            item['st_uid'] = uid
            item['st_gid'] = gid
            item.update()

    def open(self, path, flags):
        # TODO read/write locking? Permission check?
        try:
            self.table.get_item(os.path.dirname(path), name, attributes_to_get=['name'])
        except DynamoDBKeyNotFoundError:
            raise FuseOSError(ENOENT)
        else:
            return self.__allocId()

    def utimens(self, path, times=None):
        now = int(time())
        atime, mtime = times if times else (now, now)
        try:
            item = self.table.get_item(os.path.dirname(path), name)
        except DynamoDBKeyNotFoundError:
            raise FuseOSError(ENOENT)
        else:
            item['st_atime'] = atime
            item['st_mtime'] = mtime
            item.update()

    def getattr(self, path, fh=None):
        print "getattr(%s)" % path
        name=os.path.basename(path)
        if name == "":
            name="/"
        try:
            item = self.table.get_item(os.path.dirname(path), name)
        except DynamoDBKeyNotFoundError:
            raise FuseOSError(ENOENT)
        return item

    def readdir(self, path, fh=None):
        print "readdir(%s)" % path
        # Verify the directory exists
        self.__checkFileDirExists(path)

        yield '.'
        yield '..'
        items = self.table.query(path, attributes_to_get=['name'])

        # TODO Pagination
        for entry in items:
            if entry['name'] == "/":
                continue # This could be the folder itself
            yield entry['name']

    def mkdir(self, path, mode):
        print "mkdir(%s)" % path
        self.create(path, S_IFDIR | mode)

    # TODO Check if it is empty
    def rmdir(self, path):
        print "rmdir(%s)" % path
        name=os.path.basename(path)
        if name == "":
            name="/"
        self.table.get_item(os.path.dirname(path), name).delete()

    def rename(self, old, new):
        if old == new: return
        # TODO Check permissions in directories
        item = self.__checkFileExists(old)
        newItem = self.__getItemOrNone(new)
        if self.__isDirectory(newItem):
            item.hash_key = new
            item.update()
        elif self.__isFile(newItem):
            raise FuseOSError(EEXIST)
        else:
            item.hash_key = os.path.dirname(new)
            item.range_key = os.path.basename(new)
            item.update()

    def readlink(self, path):
        item = self.__checkFileExists(path)
        if not "symlink" in item:
            raise FuseOSError(EINVAL)
        return item["symlink"]

    def symlink(self, target, source):
        # TODO: Verify does not exist
        # TODO: Update parent directory time
        name=os.path.basename(target)
        if name == "":
            name="/"
        attrs={'name': name, 'path': os.path.dirname(target),
               'st_mode': S_IFLNK | 0777, 'st_nlink': 1,
               'symlink': source
               }
        item = self.table.new_item(attrs=attrs)
        item.put()
        return self.__allocId()

    def create(self, path, mode, fh=None):
        print "create(%s, %d)" % (path, mode)
        # TODO: Verify does not exist
        # TODO: Update parent directory time
        l_time=int(time())
        name=os.path.basename(path)
        if name == "":
            name="/"
        attrs={'name': name, 'path': os.path.dirname(path),
               'st_mode': mode, 'st_nlink': 1,
               'st_size': 0, 'st_ctime': l_time, 'st_mtime': l_time,
               'st_atime': l_time}
        if mode & S_IFDIR == 0:
            mode |= S_IFREG
            attrs["mode"] = mode
        item = self.table.new_item(attrs=attrs)
        item.put()
        return self.__allocId()

    def __allocId(self):
        idItem = self.table.new_item(attrs={'name':'counter', 'path':'global'})
        idItem.add_attribute("value", 1)
        res = idItem.save(return_values="ALL_NEW")
        return res["Attributes"]["value"]

    def __checkFileDirExists(self, filepath):
        self.__checkFileExists(os.path.dirname(filepath))

    def __checkFileExists(self, filepath):
        name=os.path.basename(filepath)
        if name == "":
            name="/"
        try:
            return self.table.get_item(os.path.dirname(filepath), name, attributes_to_get=['name'])
        except DynamoDBKeyNotFoundError:
            raise FuseOSError(ENOENT)

    def __getItemOrNone(self, path):
        name=os.path.basename(path)
        if name == "":
            name="/"
        try:
            return self.table.get_item(os.path.dirname(path), name, attributes_to_get=['name'])
        except DynamoDBKeyNotFoundError:
            return None

    def __isFile(self, path):
        item = self.__getItemOrNone(path)
        if item != None:
            return (item["st_mode"] & S_IFREG) == S_IFREG

    def __isDirectory(self, path):
        item = self.__getItemOrNone(path)
        if item != None:
            return (item["st_mode"] & S_IFDIR) == S_IFDIR

if __name__ == '__main__':
    if len(argv) != 4:
        print('usage: %s <region> <dynamo table> <mount point>' % argv[0])
        exit(1)

    fuse = FUSE(DynamoFS(argv[1], argv[2]), argv[3], foreground=True)
