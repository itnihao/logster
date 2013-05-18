#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import os

class Tail(object):
    def __init__(self, filename, start_pos=-1):
        self.fp = file(filename)
        self.filename = filename

        if start_pos < 0:
            self.fp.seek(-start_pos-1, 2)
            self.pos = self.fp.tell()
        else:
            self.fp.seek(start_pos)
            self.pos = start_pos

    def __iter__(self):
        counter = 0 
        while True:
            line = self.next()
            if line is None:
                counter += 1
                if counter >= 5:
                    counter = 0 
                    self.check_inode()
                time.sleep(1.0)
            else:
                yield line

    def check_inode(self):
        inode = os.stat(self.filename).st_ino
        old_inode = os.fstat(self.fp.fileno()).st_ino
        if inode != old_inode:
            self.fp = file(self.filename)
            self.pos = 0

    def next(self):
        where = self.fp.tell()
        line = self.fp.readline()
        if line and line[-1] == '\n':
            self.pos += len(line)
            return line
        else:
            self.fp.seek(where)
            return None
    def close(self):
        self.fp.close()
