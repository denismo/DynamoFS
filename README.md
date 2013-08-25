dynamo-fuse
===========

Linux FUSE file system implementation with AWS DynamoDB as the storage

Installation
============

Install it using `pip install dynamo-fuse`

Usage
=====

1. Install python-fuse:

        pip install python-fuse

2. Create an AWS Dynamo DB table in the region of your choice. The table must have Hash key named `path` and Range key named `name` (case matters, both Strings)

3. Define environment variables for AWS key - `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. At the moment only the configuration by environment variables is supported.
The user with these keys must have read/write access to the specified AWS Dynamo DB table.

4. Execute python command to mount the filesystem:

        mount -t fuse.dynamo aws:<aws region>/<dynamo table> <mount point>

    For example, if you had a table named DynamoFS in ap-southeast-2 the command would be:

        mount -t fuse.dynamo aws:ap-southeast-2/DynamoFS /mnt/dynamo

   This will mount the table to the mount point. After that you will be able to execute normal Linux file commands, such as "ls" or "mkdir".

Limitations
===========

**Note**: The implementation is almost POSIX-compliant (only 9 out of 1957 from the [fstest](http://www.tuxera.com/community/posix-test-suite/)
 test suite are failing). The unsupported commands are some rarely used options of the "chown" command.

License
=======

[GNU General Public License, version 3](http://opensource.org/licenses/gpl-3.0.html)