#!/usr/bin/env python

from __future__ import with_statement

__author__ = 'Denis Mikhalkin'

from errno import EACCES,ENOENT
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
        try:
            self.rmdir("/")
        except:
            pass
        self.mkdir("/", 0x755)

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

    def readdir(self, path, fh):
        print "readdir(%s)" % path
        yield '.'
        yield '..'
        items = self.table.query(path, attributes_to_get=['name'])

        # TODO Pagination
        for entry in items:
            if entry['name'] == "/":
                continue # This could be the folder itself
            yield entry['name']

    def __readdir(self, path, fh):
        print "readdir(%s)" % path
        item = self.table.get_item(path, range_key=None, attributes_to_get=['children'])
        if item == None:
            print "readdir: No such entry %s" % path
            raise FuseOSError(ENOENT)
        yield '.'
        yield '..'
        if not "children" in item:
            print "readdir: %s has no children" % path
            return

        for entry in item['children']:
            child = self.table.get_item(entry, range_key=None, attributes_to_get=['name'])
            if child == None:
                # Modifed underneath
                continue
            else:
                yield child.Name

    def mkdir(self, path, mode):
        print "mkdir(%s)" % path
        self.create(path, S_IFDIR | mode)

    # TODO Check if it is empty or remove everything?
    def rmdir(self, path):
        print "rmdir(%s)" % path
        name=os.path.basename(path)
        if name == "":
            name="/"
        self.table.get_item(os.path.dirname(path), name).delete()

    def create(self, path, mode, fh=None):
        print "create(%s, %d)" % (path, mode)
        id = self.__allocId()
        l_time=int(time())
        name=os.path.basename(path)
        if name == "":
            name="/"
        attrs={'name': name, 'path': os.path.dirname(path),
               'st_mode': mode, 'st_nlink': 1,
               'st_size': 0, 'st_ctime': l_time, 'st_mtime': l_time,
               'st_atime': l_time, 'id': id}
        if mode & S_IFDIR == 0:
            mode |= S_IFREG
            attrs["mode"] = mode
        item = self.table.new_item(attrs=attrs)
        item.put()
        return id

    def __allocId(self):
        idItem = self.table.new_item(attrs={'name':'counter', 'path':'global'})
        idItem.add_attribute("value", 1)
        res = idItem.save(return_values="ALL_NEW")
        return res["Attributes"]["value"]

if __name__ == '__main__':
    if len(argv) != 4:
        print('usage: %s <region> <dynamo table> <mountpoint>' % argv[0])
        exit(1)

    fuse = FUSE(DynamoFS(argv[1], argv[2]), argv[3], foreground=True)
