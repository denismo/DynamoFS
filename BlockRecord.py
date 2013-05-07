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

__author__ = 'Denis Mikhalkin'

BLOCK_SIZE = 32768

from errno import EACCES, ENOENT, EINVAL, EEXIST, EOPNOTSUPP, EIO, EAGAIN
from os.path import realpath
from threading import Lock
import boto.dynamodb
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time
from boto.dynamodb.condition import EQ, GT
from boto.dynamodb.types import Binary
import os
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging
import sys
import cStringIO

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class BlockRecord:
    log = logging.getLogger("dynamo-fuse-block")
    item = None

    def __init__(self, item):
        self.item = item

    def __getitem__(self, key):
        return self.item[key]

    def __setitem__(self, key, value):
        self.item[key] = value
        return value

    def __contains__(self, item):
        return item in self.item

    def save(self):
        self.item.save()

    def writeData(self, startOffset, dataSlice):
        if "data" in self.item:
            self.log.debug("write block %s has data", self.item["name"])
            itemData = self.item["data"].value
            self.item['data'] = Binary(itemData[0:startOffset] + dataSlice + itemData[startOffset + len(dataSlice):])
        else:
            self.log.debug("write block %s has NO data", self.item["name"])
            self.item['data'] = Binary(dataSlice)
