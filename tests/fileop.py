from sys import argv, exit
import fcntl
import os
import sys
import traceback

__author__ = 'Denis Mikhalkin'

if __name__ == '__main__':
    filePath = argv[1]
    mode = argv[2]
    lock = argv[3]

    file = open(filePath, mode)
    try:
        if lock is not None and lock != "None":
            fcntl.lockf(file, int(lock))

        if mode == "r":
            file.read(1)
        elif mode == "w+":
            file.write("a")
        elif mode == "rw":
            file.read(1)
            file.write("a")
    except Exception, e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        exit(-1)

    file.close()
    exit(0)
