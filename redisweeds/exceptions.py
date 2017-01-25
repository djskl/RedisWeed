# -*- coding: utf-8 -*-
# vi:si:et:sw=4:sts=4:ts=4

class BadFidFormat(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class RedisConnError(Exception):

    def __init__(self, host, port):
        self.host, self.port = host, port

    def __str__(self):
        return "Can't connect to redis(host: {host}, port: {port})".format(
            host = self.host,
            port = self.port
        )
