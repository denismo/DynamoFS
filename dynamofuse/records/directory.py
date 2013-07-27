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

from dynamofuse.base import BaseRecord
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
from posix import *

if not hasattr(__builtins__, 'bytes'):
    bytes = str

class Directory(BaseRecord):
    def getattr(self):
        self.record["st_nlink"] = 1
        self.record["st_size"] = 0
        return self.record

    def list(self):
        items = self.accessor.tablev2.query(path__eq=self.path, attributes=['name', 'deleted', 'hidden'])

        for entry in items:
            if entry['name'] == "/" or ("deleted" in entry and entry['deleted']) or ('hidden' in entry):
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
        return sum(
            1 for entry in self.accessor.tablev2.query(path__eq=self.path, attributes=['name', 'deleted']) if entry[
                                                                                                              'name'] != "/" and not (
                "deleted" in entry and entry['deleted'])) == 0

    def isSticky(self):
        return self.record['st_mode'] & S_ISVTX