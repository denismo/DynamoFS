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
from boto.dynamodb.exceptions import DynamoDBConditionalCheckFailedError

__author__ = 'Denis Mikhalkin'

from errno import EAGAIN, EFAULT
from fuse import FuseOSError
import os
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging
import cStringIO
import uuid
from time import time, sleep
from threading import Lock, current_thread

if not hasattr(__builtins__, 'bytes'):
    bytes = str

MAX_LOCK_RETRIES = 5
lockLog = logging.getLogger("dynamo-fuse-lock  ")

class DynamoLock:
    log = logging.getLogger("dynamo-fuse-lock  ")

    def __init__(self, path, accessor, item):
        self.path = path
        self.accessor = accessor
        self.lockId = uuid.uuid4().hex
        self.item = item
        self.acquired = 0

    def __enter__(self):
        if self.acquired:
            self.acquired += 1
            self.log.debug(" Reentrant lock %d", self.acquired)
            return

        self.log.debug(" Acquiring exclusive lock on %s", self.path)
        item = self.accessor.newItem(attrs={
            "path": os.path.dirname(self.path),
            "name": os.path.basename(self.path)
        })
        item.put_attribute('lockOwner', self.lockId)
        retries = 0
        while retries < MAX_LOCK_RETRIES:
            try:
                item.save(expected_value={'lockOwner': False})
                self.log.debug(" Got the lock on %s", self.path)
                self.acquired += 1
                return
            except DynamoDBConditionalCheckFailedError:
                # Somone acquired the lock before us
                sleep(1)
                retries += 1

        self.log.debug(" CANNOT lock %s", self.path)
        self.__exit__()
        raise FuseOSError(EAGAIN)

    def __exit__(self, type=None, value=None, traceback=None):
        self.acquired -= 1
        if self.acquired > 0:
            self.log.debug(" Reentrant exit %d", self.acquired)
            return

        self.log.debug(" Releasing exclusive lock on %s", self.path)
        if 'recordDeleted' in self.item.record:
            self.log.debug(" Not saving lock - item %s was deleted", self.path)
        else:
            item = self.accessor.newItem(attrs={
                "path": os.path.dirname(self.path),
                "name": os.path.basename(self.path),
                })
            item.delete_attribute("lockOwner")
            item.save(expected_value={'lockOwner': self.lockId})


class DynamoReadLock:
    log = logging.getLogger("dynamo-fuse-lock  ")

    def __init__(self, path, accessor, item):
        self.path = path
        self.accessor = accessor
        self.item = item
        self.acquired = 0

    def __enter__(self):
        if self.acquired:
            self.acquired += 1
            self.log.debug("   Reentrant read lock %d", self.acquired)
            return

        self.log.debug("   Acquiring read lock on %s", self.path)
        item = self.accessor.newItem(attrs={
            "path": os.path.dirname(self.path),
            "name": os.path.basename(self.path)
        })
        item.add_attribute('readLock', 1)
        retries = 0
        while retries < MAX_LOCK_RETRIES or hasattr(self, 'wait'):
            try:
                item.save(expected_value={'writeLock': False})
                self.log.debug(" Got the read lock on %s", self.path)
                self.acquired += 1
                return
            except DynamoDBConditionalCheckFailedError:
                # Somone acquired the write lock before us
                sleep(1)
                retries += 1

        self.log.debug("   CANNOT read lock %s", self.path)
        self.__exit__()
        raise FuseOSError(EAGAIN)

    def lock(self, wait=False):
        if wait: self.wait = wait
        try:
            self.__enter__()
        finally:
            del self.wait

    def unlock(self):
        self.__exit__()

    @staticmethod
    def unlockStatic(item):
        if not 'writeLock' in item.record:
            lockLog.debug('   Unlocking read lock on %s', item.path)
            newItem = item.accessor.newItem(attrs={
                "path": os.path.dirname(item.path),
                "name": os.path.basename(item.path),
                })
            newItem.add_attribute('readLock', -1)
            newItem.save()
            return True
        else:
            return False

    def __exit__(self, type=None, value=None, traceback=None):
        self.acquired -= 1
        if self.acquired > 0:
            self.log.debug("   Reentrant read lock exit %d", self.acquired)
            return

        self.log.debug("   Releasing read lock on %s", self.path)
        if 'recordDeleted' in self.item.record:
            self.log.debug(" Not saving read lock - item %s was deleted", self.path)
        else:
            item = self.accessor.newItem(attrs={
                "path": os.path.dirname(self.path),
                "name": os.path.basename(self.path),
                })
            item.add_attribute('readLock', -1)
            item.save()


class DynamoWriteLock:
    log = logging.getLogger("dynamo-fuse-lock  ")

    def __init__(self, path, accessor, item):
        self.path = path
        self.accessor = accessor
        self.item = item
        self.acquired = 0
        self.lockId = uuid.uuid4().hex

    def lock(self, wait=False):
        if wait: self.wait = wait
        try:
            self.__enter__()
        finally:
            del self.wait

    def unlock(self):
        self.__exit__()

    @staticmethod
    def unlockStatic(item):
        if 'writeLock' in item.record:
            lockLog.debug('    Unlocking write lock on %s', item.path)
            newItem = item.accessor.newItem(attrs={
                "path": os.path.dirname(item.path),
                "name": os.path.basename(item.path),
                })
            newItem.delete_attribute('writeLock')
            newItem.save(expected_value={'writeLock': item.record['writeLock']})
            return True
        else:
            return False

    def __enter__(self):
        if self.acquired:
            self.acquired += 1
            self.log.debug("   Reentrant write lock %d", self.acquired)
            return

        self.log.debug("   Acquiring write lock on %s", self.path)
        item = self.accessor.newItem(attrs={
            "path": os.path.dirname(self.path),
            "name": os.path.basename(self.path)
        })
        item.put_attribute('writeLock', self.lockId)
        retries = 0
        while retries < MAX_LOCK_RETRIES or hasattr(self, 'wait'):
            try:
                item.save(expected_value={'writeLock': False, 'readLock': 0})
                self.log.debug("   Got the write lock on %s", self.path)
                self.acquired += 1
                return
            except DynamoDBConditionalCheckFailedError:
                # Somone acquired the write lock before us
                sleep(1)
                retries += 1

        self.log.debug("   CANNOT write lock %s", self.path)
        self.__exit__()
        raise FuseOSError(EAGAIN)

    def __exit__(self, type=None, value=None, traceback=None):
        self.acquired -= 1
        if self.acquired > 0:
            self.log.debug("   Reentrant write lock exit %d", self.acquired)
            return

        self.log.debug("   Releasing write lock on %s", self.path)
        if 'recordDeleted' in self.item.record:
            self.log.debug("   Not saving write lock - item %s was deleted", self.path)
        else:
            item = self.accessor.newItem(attrs={
                "path": os.path.dirname(self.path),
                "name": os.path.basename(self.path),
                })
            item.delete_attribute('writeLock')
            item.save(expected_value={'writeLock': self.lockId})

    @classmethod
    def applyLock(cls, attrs):
        attrs['writeLock'] = uuid.uuid4().hex


class FileLockManager:

    def __init__(self):
        self.fileDict = dict()
        self.fileDictLock = Lock()

    def create(self, path):
        self.fileDictLock.acquire()
        FileLock.dump()
        try:
            if not self.fileDict.has_key(path):
                lockLog.debug("    file lock %s - created", path)
                self.fileDict[path] = FileLock(path)
            else:
                fileLock = self.fileDict[path]
                fileLock.acquire()
        finally:
            self.fileDictLock.release()
        FileLock.dump()

    def release(self, path):
        self.fileDictLock.acquire()
        FileLock.dump()
        try:
            fileLock = self.fileDict[path]
            if not fileLock.release():
                lockLog.debug("    file lock %s - deleting", path)
                del self.fileDict[path]
        finally:
            self.fileDictLock.release()
        FileLock.dump()

        """Returns true if the process acquired lock, and if it should be persisted in Dynamo
        False otherwise"""

    def lock(self, path, pid):
        FileLock.dump()
        fileLock = self.getFileLock(path)
        try:
            with fileLock:
                return fileLock.lock(pid)
        finally:
            FileLock.dump()

    def unlock(self, path, pid):
        FileLock.dump()
        fileLock = self.getFileLock(path)
        try:
            with fileLock:
                return fileLock.unlock(pid)
        finally:
            FileLock.dump()

    def getFileLock(self, path):
        return self.fileDict[path]


class FileLock:
    locksHandle = None
    @classmethod
    def dump(cls):
        pass
#        if FileLock.locksHandle is not None:
#            lockLog.debug("locking dump on %s: %s", current_thread().ident, repr(FileLock.locksHandle))
#        else:
#            lockLog.debug("locking dump on %s - no locksHandle", current_thread().ident)

    def __init__(self, path):
        self.path = path
        self.locks = dict()
        self.__objectLock = Lock()
        self.counter = 1

#        if path == "/a":
#            if FileLock.locksHandle is not None: lockLog.debug("    file lock - locksHandle is not None")
#            FileLock.locksHandle = self.locks

    def __enter__(self):
        self.__objectLock.acquire(True)

    def __exit__(self, type=None, value=None, traceback=None):
        self.__objectLock.release()

    def lock(self, pid):
        if pid in self.locks:
            lockLog.debug("    file lock %s %d - already locked", self.path, pid)
            return False
        self.locks[pid] = True
        lockLog.debug("    file lock %s %d - locking %s", self.path, pid, id(self))
        return True

    def unlock(self, pid):
        if pid not in self.locks:
            lockLog.debug("    file lock %s %d - not locked", self.path, pid)
            return False
        lockLog.debug("    file lock %s %d - unlocking %s", self.path, pid, id(self))
        del self.locks[pid]
#        if pid in self.locks: lockLog.debug("    file lock - pid %d is still there", pid)
#        lockLog.debug("    file lock after unlock %s", self.path)
        return True

    def acquire(self):
        self.counter += 1

    def release(self):
        self.counter -= 1
        return self.counter