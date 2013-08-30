import sys

__author__ = 'Denis Mikhalkin'

import unittest
import fcntl
import os
from time import sleep
from subprocess import Popen

class TestLocks(unittest.TestCase):
    READ_FILE = os.path.join(os.getcwd(), "test.txt")

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(TestConcurrentLocks.READ_FILE):
            os.unlink(TestLocks.READ_FILE)

    def setUp(self):
        if not os.path.exists(TestLocks.READ_FILE):
            file = open(TestLocks.READ_FILE, "w")
            file.write("aaaa")
            file.close()
        vfs = os.statvfs(TestLocks.READ_FILE)
        self.isDynamo = vfs.f_blocks == (sys.maxint-1)

    def testReadWithLock(self):
        file = open(TestLocks.READ_FILE, "r", False)
        self.assertIsNotNone(file)
        self.assertIsNotNone(file.read(1))
        fcntl.lockf(file, fcntl.LOCK_SH)
        self.assertIsNotNone(file.read(1))
        file.close()

    def testWriteReadOnlyWithLock(self):
        file = open(TestLocks.READ_FILE, "r", False)
        self.assertIsNotNone(file)
        self.assertIsNotNone(file.read(1))
        fcntl.lockf(file, fcntl.LOCK_SH)
        with self.assertRaises(IOError):
            file.write("a")
        file.close()

    def testWriteWithSharedLock(self):
        file = open(TestLocks.READ_FILE, "w+", False)
        self.assertIsNotNone(file)
        file.write("a")
        fcntl.lockf(file, fcntl.LOCK_SH)
        # Ideally, we can implement upgradable locks. However, it is a low priority
        if self.isDynamo:
            with self.assertRaises(IOError):
                file.write("a")
        else:
            file.write("a")
        file.close()

    def testReadWriteWithSharedLock(self):
        file = open(TestLocks.READ_FILE, "w+", False)
        self.assertIsNotNone(file)
        file.write("a")
        self.assertIsNotNone(file.read(1))
        fcntl.lockf(file, fcntl.LOCK_SH)
        # Ideally, we can implement upgradable locks. However, it is a low priority
        if self.isDynamo:
            with self.assertRaises(IOError):
                file.write("a")
        else:
            file.write("a")
        self.assertIsNotNone(file.read(1))
        file.close()

    def testWriteWithExclusiveLock(self):
        file = open(TestLocks.READ_FILE, "w+", False)
        self.assertIsNotNone(file)
        file.write("a")
        fcntl.lockf(file, fcntl.LOCK_EX)
        file.write("a")
        file.close()

    def testReadWriteWithExclusiveLock(self):
        file = open(TestLocks.READ_FILE, "w+", False)
        self.assertIsNotNone(file)
        file.write("a")
        self.assertIsNotNone(file.read(1))
        fcntl.lockf(file, fcntl.LOCK_EX)
        file.write("a")
        self.assertIsNotNone(file.read(1))
        file.close()

class TestConcurrentLocks(unittest.TestCase):
    READ_FILE = os.path.join(os.getcwd(), "test.txt")

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(TestConcurrentLocks.READ_FILE):
            os.unlink(TestConcurrentLocks.READ_FILE)

    def setUp(self):
        if not os.path.exists(TestConcurrentLocks.READ_FILE):
            file = open(TestConcurrentLocks.READ_FILE, "w")
            file.write("aaaa")
            file.close()
        vfs = os.statvfs(TestLocks.READ_FILE)
        self.isDynamo = vfs.f_blocks == (sys.maxint-1)

    def __anotherProcessRead(self, path, lock=None):
        return self.__anotherProcessCall(path, "r", lock)

    def __anotherProcessWrite(self, path, lock=None):
        return self.__anotherProcessCall(path, "w+", lock)

    def __anotherProcessDelete(self, path):
        return self.__anotherProcessCall(path, "delete")

    def __anotherProcessTruncate(self, path):
        return self.__anotherProcessCall(path, "w")

    def __anotherProcessRename(self, path):
        return self.__anotherProcessCall(path, "rename")

    def __anotherProcessCall(self, path, mode, lock=None):
        proc = Popen(["python", os.path.dirname(sys.argv[0]) + "/fileop.py", path, mode, str(lock)])
        counter = 0
        proc.poll()
        while proc.returncode is None and counter < 10:
            proc.poll()
            if proc.returncode is not None: break
            sleep(0.2)
            counter += 1
        if proc.returncode is not None:
            return proc.returncode
        else:
            proc.kill()
#            print "Timeout waiting for child process"
            return -1

    def testConcurrentRead(self):
        f = open(TestConcurrentLocks.READ_FILE, "r")
        self.assertIsNotNone(f.read(1))
        self.assertEquals(0, self.__anotherProcessRead(TestConcurrentLocks.READ_FILE))
        f.close()

    def testConcurrentReadWithSharedLock(self):
        f = open(TestConcurrentLocks.READ_FILE, "r")
        fcntl.lockf(f, fcntl.LOCK_SH)
        self.assertIsNotNone(f.read(1))
        self.assertEquals(0, self.__anotherProcessRead(TestConcurrentLocks.READ_FILE))
        f.close()

    def testConcurrentReadWithSharedDoubleLock(self):
        f = open(TestConcurrentLocks.READ_FILE, "r")
        fcntl.lockf(f, fcntl.LOCK_SH)
        self.assertIsNotNone(f.read(1))
        self.assertEquals(0, self.__anotherProcessRead(TestConcurrentLocks.READ_FILE, fcntl.LOCK_SH))
        f.close()

    def testConcurrentReadWithSharedOtherLock(self):
        f = open(TestConcurrentLocks.READ_FILE, "r")
        self.assertIsNotNone(f.read(1))
        self.assertEquals(0, self.__anotherProcessRead(TestConcurrentLocks.READ_FILE, fcntl.LOCK_SH))
        f.close()

    def testConcurrentWrite(self):
        f = open(TestConcurrentLocks.READ_FILE, "w+")
        f.write("a")
        self.assertEquals(0, self.__anotherProcessWrite(TestConcurrentLocks.READ_FILE))
        f.close()

    def testConcurrentWriteWithExLock(self):
        f = open(TestConcurrentLocks.READ_FILE, "w+")
        fcntl.lockf(f, fcntl.LOCK_EX)
        f.write("a")
        self.assertEquals(-1 if self.isDynamo else 0, self.__anotherProcessWrite(TestConcurrentLocks.READ_FILE))
        f.close()

    def testConcurrentWriteWithDoubleExLock(self):
        f = open(TestConcurrentLocks.READ_FILE, "w+")
        fcntl.lockf(f, fcntl.LOCK_EX)
        f.write("a")
        self.assertEquals(-1, self.__anotherProcessWrite(TestConcurrentLocks.READ_FILE, fcntl.LOCK_EX))
        f.close()

    def testDeleteDuringRead(self):
        f = open(TestConcurrentLocks.READ_FILE, "r")
        self.assertIsNotNone(f.read(1))
        self.assertEquals(0, self.__anotherProcessDelete(TestConcurrentLocks.READ_FILE))
        f.close()

    def testDeleteDuringReadLock(self):
        f = open(TestConcurrentLocks.READ_FILE, "r")
        fcntl.lockf(f, fcntl.LOCK_SH)
        self.assertIsNotNone(f.read(1))
        self.assertEquals(-1, self.__anotherProcessDelete(TestConcurrentLocks.READ_FILE))
        f.close()

    def testDeleteDuringWrite(self):
        f = open(TestConcurrentLocks.READ_FILE, "w+")
        f.write("a")
        self.assertEquals(0, self.__anotherProcessDelete(TestConcurrentLocks.READ_FILE))
        f.close()

    def testDeleteDuringWriteLock(self):
        f = open(TestConcurrentLocks.READ_FILE, "w+")
        fcntl.lockf(f, fcntl.LOCK_EX)
        f.write("a")
        self.assertEquals(-1, self.__anotherProcessDelete(TestConcurrentLocks.READ_FILE))
        f.close()

    def testTruncateDuringRead(self):
        f = open(TestConcurrentLocks.READ_FILE, "r")
        self.assertIsNotNone(f.read(1))
        self.assertEquals(0, self.__anotherProcessTruncate(TestConcurrentLocks.READ_FILE))
        f.close()

    def testTruncateDuringReadLock(self):
        f = open(TestConcurrentLocks.READ_FILE, "r")
        fcntl.lockf(f, fcntl.LOCK_SH)
        self.assertIsNotNone(f.read(1))
        self.assertEquals(-1, self.__anotherProcessTruncate(TestConcurrentLocks.READ_FILE))
        f.close()

    def testTruncateDuringWrite(self):
        f = open(TestConcurrentLocks.READ_FILE, "w+")
        f.write("a")
        self.assertEquals(0, self.__anotherProcessTruncate(TestConcurrentLocks.READ_FILE))
        f.close()

    def testTruncateDuringWriteLock(self):
        f = open(TestConcurrentLocks.READ_FILE, "w+")
        fcntl.lockf(f, fcntl.LOCK_EX)
        f.write("a")
        self.assertEquals(-1, self.__anotherProcessTruncate(TestConcurrentLocks.READ_FILE))
        f.close()

    def testRenameDuringRead(self):
        f = open(TestConcurrentLocks.READ_FILE, "r")
        self.assertIsNotNone(f.read(1))
        self.assertEquals(0, self.__anotherProcessRename(TestConcurrentLocks.READ_FILE))
        f.close()

    def testRenameDuringReadLock(self):
        f = open(TestConcurrentLocks.READ_FILE, "r")
        fcntl.lockf(f, fcntl.LOCK_SH)
        self.assertIsNotNone(f.read(1))
        self.assertEquals(-1, self.__anotherProcessRename(TestConcurrentLocks.READ_FILE))
        f.close()

    def testRenameDuringWrite(self):
        f = open(TestConcurrentLocks.READ_FILE, "w+")
        f.write("a")
        self.assertEquals(0, self.__anotherProcessRename(TestConcurrentLocks.READ_FILE))
        f.close()

    def testRenameDuringWriteLock(self):
        f = open(TestConcurrentLocks.READ_FILE, "w+")
        fcntl.lockf(f, fcntl.LOCK_EX)
        f.write("a")
        self.assertEquals(-1, self.__anotherProcessRename(TestConcurrentLocks.READ_FILE))
        f.close()

if __name__ == '__main__':
    unittest.main()