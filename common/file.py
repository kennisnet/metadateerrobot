# -*- coding: utf-8 -*-

""" Returns a memory mapped file """
def mapFile(filename):
    import mmap
    import os

    file = open(filename, "r+")
    size = os.path.getsize(filename)
    return mmap.mmap(file.fileno(), size, prot=mmap.PROT_READ)
