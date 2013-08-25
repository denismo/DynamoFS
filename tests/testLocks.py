__author__ = 'Denis Mikhalkin'

import unittest
import fcntl
import os

class TestLocks(unittest.TestCase):
    READ_FILE = "test.txt"
    @classmethod
    def setUpClass(cls):
        if not os.path.exists(TestLocks.READ_FILE):
            file = open(TestLocks.READ_FILE, "w")
            file.write("aaaa")
            file.close()

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
        file.write("a")
        file.close()

    def testReadWriteWithSharedLock(self):
        file = open(TestLocks.READ_FILE, "w+", False)
        self.assertIsNotNone(file)
        file.write("a")
        self.assertIsNotNone(file.read(1))
        fcntl.lockf(file, fcntl.LOCK_SH)
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

if __name__ == '__main__':
    unittest.main()