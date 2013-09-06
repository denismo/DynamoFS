DynamoFS has been tested using various mechanisms:
- fstest test suite [fstest](http://www.tuxera.com/community/posix-test-suite/)
- specific lock tests (see [tests/testLocks.py])
- random concurrent stress tests (see [tests/filemonkey.py])

fstest test suite
=================

This suite is used by many file system developers to verify their file system functionality, stability and POSIX compliance.
While not a certification suite it provides good coverage of the majority of file system functions. The version I'm using originates from Tuxera -
the developers of the NTFS file system driver for Linux.

At the moment DynamoFS passes most of the tests, failing on 9 out of 1957 test cases. The failing test cases are some rare combinatios of parameters for the "chown" command -
the ones to do with the assertions during "chown" on users which belong to multiple groups. Note that all other commands work with multiple groups without problems.

Specific locks tests
====================

This is a test suite written to verify the correctness of locking operations with regards to all other file update operations.
It ensures that the same file can be locked, read/written, renamed, truncated, deleted in a consistent manner.

FileMonkey
====================

This stress test suite executes a number of file read and update operations on the file system in a highly concurrent setup, and then verifies that the file system is left in a consistent state after that.
This allows to simulate the load which can be produced by many clients accessing the file system in parallel.


