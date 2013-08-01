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
from dynamofuse.lock import DynamoLock

__author__ = 'Denis Mikhalkin'

from errno import *
from os.path import realpath
from sys import argv, exit
from threading import Lock
import boto.dynamodb
from posix import F_OK, R_OK
from posix import R_OK, X_OK, W_OK
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError, DynamoDBConditionalCheckFailedError
from boto.exception import BotoServerError, BotoClientError
from boto.exception import DynamoDBResponseError
from stat import *
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

MAX_RETRIES = 5
DELETED_LINKS="$deleted$"
CONSISTENT_OPER=False

def retry(m):
    def wrappedM(*args):
        retries = 0
        logger = logging.getLogger("dynamo-fuse")
        while retries < MAX_RETRIES:
            try:
                return m(*args)
            except DynamoDBConditionalCheckFailedError, cf:
                logger.debug(cf)
                logger.debug("Retrying " + str(m))
                retries += 1
                if retries >= MAX_RETRIES:
                    raise FuseOSError(EIO)
    return wrappedM

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
                    'st_gid': gid, 'st_uid': uid,
                    'version': 1,
                    'st_ino': attrs['st_ino'] if 'st_ino' in attrs else self.accessor.allocUniqueId()
        }
        for k, v in attrs.items():
            newAttrs[k] = v
        self.log.debug("Create attrs: %s", newAttrs)
        allowOverwrite = "allowOverwrite" in attrs
        if allowOverwrite:
            del attrs["allowOverwrite"]
            item = self.accessor.table.new_item(attrs=newAttrs)
            item.put()
        else:
            item = self.accessor.table.new_item(attrs=newAttrs)
            item.put()

        self.record = item
        logging.getLogger("dynamo-fuse-record").debug("Read record %s, version %d", os.path.join(self.record["path"], self.record["name"]), self.record["version"])
        self.record.save = self.safeSave(self.record, self.record.save)

    def init(self, accessor, path, record):
        self.accessor = accessor
        self.path = path
        self.record = record
        logging.getLogger("dynamo-fuse-record").debug("Read record %s, version %d", os.path.join(record["path"], record["name"]), record["version"])
        self.record.save = self.safeSave(self.record, self.record.save)

    @staticmethod
    def safeSave(record, origSave):
        def safeSaveImpl(**kwargs):
            logging.getLogger("dynamo-fuse-record").debug("Saving record %s, version %d", os.path.join(record["path"], record["name"]), record["version"])
            record.add_attribute("version", 1)
            return origSave(expected_value={"version": record["version"]}, **kwargs)
        return safeSaveImpl

    def getRecord(self):
        return self.record

    def delete(self):
        self.record.delete()

        self.updateDirectoryMCTime(self.path)

    def moveTo(self, newPath, forceUpdate=False):
        self.cloneItem(newPath)

        self.delete()

    def cloneItem(self, path, attrsToPreserve=('type', 'st_nlink', 'st_size', 'st_ino', 'st_dev', 'st_rdev', 'st_mode', 'blockId', 'st_gid', 'st_uid', 'deleted', 'blockId')):
        attrs=dict(self.record)
        del attrs['name']
        del attrs['path']
        if attrsToPreserve:
            toDelete = []
            for attr in attrs.keys():
                if not attr in attrsToPreserve:
                    toDelete.append(attr)
            for attr in toDelete:
                del attrs[attr]
        newItem = self.__class__()
        attrs["allowOverwrite"] = True
        newItem.create(self.accessor, path, attrs)

        self.updateDirectoryMTime(path)
        return newItem

    def getattr(self):
        return self.getRecord()

    def isHidden(self):
        return 'hidden' in self.getRecord()

    @retry
    def chmod(self, mode):
        block = self.getRecord()

        self.permitPrivilegedOrOwner(block)

        block['st_mode'] &= 0770000
        block['st_mode'] |= mode
        self.checkSetGid(block)
        block['st_ctime'] = max(int(time()), block['st_ctime'])
        block.save()

    @retry
    def chown(self, uid, gid):
        block = self.getRecord()

        self.permitPrivilegedOnly(block, uid, gid)
        self.permitOwnerToGroup(block, uid, gid)

        if uid != -1: block['st_uid'] = uid
        if gid != -1: block['st_gid'] = gid
        block['st_ctime'] = max(int(time()), block['st_ctime'])
        block.save()

    def updateCTime(self):
        self.record['st_ctime'] = max(self.record['st_ctime'], int(time()))
        # TODO Concurrency
        self.record.save()

    def updateMTime(self):
        self.record['st_mtime'] = max(self.record['st_mtime'], int(time()))
        # TODO Concurrency
        self.record.save()

    def updateMCTime(self):
        l_time = int(time())
        self.record['st_mtime'] = max(self.record['st_mtime'], l_time)
        self.record['st_ctime'] = max(self.record['st_mtime'], l_time)
        # TODO Concurrency
        self.record.save()

    @retry
    def updateDirectoryMTime(self, filepath):
        dir = self.accessor.getRecordOrThrow(os.path.dirname(filepath))
        dir.updateMTime()

    @retry
    def updateDirectoryMCTime(self, filepath):
        dir = self.accessor.getRecordOrThrow(os.path.dirname(filepath))
        dir.updateMCTime()

    def isFile(self):
        return self.record["type"] == "File"

    def isDirectory(self):
        return self.record["type"] == "Directory"

    @staticmethod
    def isDirectoryItem(item):
        return "type" in item and item['type'] == 'Directory'

    def isLink(self):
        return self.record["type"] == "Symlink"

    def isHardLink(self):
        return self.record["type"] == "Link"

    def isNode(self):
        return self.record["type"] == "Node"

    def getOwner(self):
        return self.getRecord()['st_uid']

    def getParent(self, fs):
        return fs.getRecordOrThrow(os.path.dirname(self.path))

    def takeLock(self):
        return DynamoLock(self.path, self.accessor, self)

    def __getitem__(self, item):
        return self.record[item]

    def __setitem__(self, key, value):
        self.record[key] = value

    def __contains__(self, item):
        return self.record.__contains__(item)

    def save(self):
        self.record.save()

    def access(self, mode):
        if mode == F_OK:
            return 0

        block = self.getRecord()
        st_mode = block['st_mode']
        st_uid = block['st_uid']
        st_gid = block['st_gid']
        return 0 if self.modeAccess(mode, st_mode, st_uid, st_gid) else -1

    def modeToStr(self, mode):
        res = ""
        if mode & S_IRUSR: res += 'r'
        else: res += '-'
        if mode & S_IWUSR: res += 'w'
        else: res += '-'
        if mode & S_IXUSR: res += 'x'
        else: res += '-'

        if mode & S_IRGRP: res += 'r'
        else: res += '-'
        if mode & S_IWGRP: res += 'w'
        else: res += '-'
        if mode & S_IXGRP: res += 'x'
        else: res += '-'

        if mode & S_IROTH: res += 'r'
        else: res += '-'
        if mode & S_IWOTH: res += 'w'
        else: res += '-'
        if mode & S_IXOTH: res += 'x'
        else: res += '-'

        return res

    def modeAccess(self, mode, st_mode, st_uid, st_gid):
        (uid, gid, unused) = fuse_get_context()
#        self.log.debug("modeAccess, node %x(%s) %d %d, input %x %d %d", st_mode, self.modeToStr(st_mode), st_uid, st_gid, mode, uid, gid)
        if mode & R_OK:
            if not (not uid or uid == st_uid and st_mode & S_IRUSR or uid != st_uid and gid == st_gid and st_mode & S_IRGRP or uid != st_uid and gid != st_gid and st_mode & S_IROTH):
                return False

        if mode & W_OK:
            if not (not uid or uid == st_uid and st_mode & S_IWUSR or uid != st_uid and gid == st_gid and st_mode & S_IWGRP or uid != st_uid and gid != st_gid and st_mode & S_IWOTH):
                return False

        if mode & X_OK:
            if not (not uid or uid == st_uid and st_mode & S_IXUSR or uid != st_uid and gid == st_gid and st_mode & S_IXGRP or uid != st_uid and gid != st_gid and st_mode & S_IXOTH):
                return False

        return True

    @retry
    def utimens(self, atime, mtime):
        block = self.getRecord()
        block['st_atime'] = max(int(atime), block['st_atime'])
        block['st_mtime'] = max(int(mtime), block['st_mtime'])
        block.save()

    def permitPrivilegedOnly(self, block, uid, gid):
        (ouid, unused, unused) = fuse_get_context()
        if ouid and not (uid == -1 or uid == block['st_uid']):
            self.log.debug('permitPrivilegedOnly: owner is not privileged %d => %d, owner %d', block['st_uid'], uid, ouid)
            raise FuseOSError(EPERM)

    def permitOwnerToGroup(self, block, uid, gid):
        if block['st_gid'] != gid and gid != -1:
            (ouid, ogid, unused) = fuse_get_context()
            if block['st_uid'] != ouid and ouid or gid != ogid and ogid:
                self.log.debug('permitOwnerToGroup: not allowed %d != %d or %d != %d', block['st_uid'], ouid, gid, ogid)
                raise FuseOSError(EPERM)

    def permitPrivilegedOrOwner(self, block):
        (ouid, unused, unused) = fuse_get_context()
        if block['st_uid'] != ouid and ouid:
            raise FuseOSError(EPERM)

    def checkSetGid(self, block):
        (ouid, ogid, unused) = fuse_get_context()
        if ouid and ogid != block['st_gid']:
            block['st_mode'] &= ~S_ISGID

    def isDeleted(self):
        return False