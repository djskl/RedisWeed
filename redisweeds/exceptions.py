# -*- coding: utf-8 -*-
# vi:si:et:sw=4:sts=4:ts=4

class BadFidFormat(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class FileNotExists(Exception):
    def __init__(self, filename):
        self.filename = filename

    def __str__(self):
        return "No such file in the weed: '%s'"%self.filename


class WeedDuplicateError(Exception):

    def __init__(self, filename):
        self.filename = filename

    def __str__(self):
        return "File: '%s' has existed"%self.filename


class NullNotAllowed(Exception):
    def __str__(self):
        return "empty argument is not allowed"

class WeedInternalError(Exception):
    pass