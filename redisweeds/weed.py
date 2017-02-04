#encoding: utf-8
import io, os
import redis
from redisweeds.base import WeedFS
from redisweeds.exceptions import FileNotExists, WeedDuplicateError, WeedInternalError
from redisweeds.utils import NotNullArgsRequired, make_weed_key


class RedisWeed(WeedFS):
    def __init__(self, redis_host="localhost", redis_port=6379,
                 master_addr='localhost', master_port=9333, use_session=False):
        super(RedisWeed, self).__init__(master_addr, master_port, use_session)
        self.rdb = redis.StrictRedis(host=redis_host, port=redis_port)

    @NotNullArgsRequired
    def read(self, filename):
        filekey = self.rdb.get(filename)
        if not filekey:
            raise FileNotExists(filename)
        _cnt = self.get_file(filekey)
        return _cnt

    @NotNullArgsRequired
    def write(self, filename, file_cnt, overwrite=False, timeout=0):
        filekey = make_weed_key(filename)
        if self.rdb.exists(filekey) and not overwrite:
            raise WeedDuplicateError(filename)

        if self.rdb.exists(filekey) and overwrite:
            self.delete(filename)

        bytes_stream = io.BytesIO()
        bytes_stream.write(file_cnt)
        bytes_stream.seek(0)

        fid = self.upload_file(name=filekey, stream=bytes_stream)

        if not fid:
            raise WeedInternalError()

        if timeout > 0:
            #设置一个shadowkey是因为redis响应expire事件时不能将过期的key对应的值发送给响应函数
            #需要处理函数通过这个shadowkey来间接获取
            self.rdb.set("shadow:%s" % filekey, fid)
            self.rdb.setex(filekey, timeout, fid)
        else:
            self.rdb.set(filekey, fid)

    @NotNullArgsRequired
    def upload(self, filepath, overwrite=True, timeout=0):
        if not os.path.exists(filepath):
            raise IOError()

        filename = os.path.basename(filepath)
        filekey = make_weed_key(filename)

        if self.rdb.exists(filekey) and not overwrite:
            raise WeedDuplicateError(filename)

        if self.rdb.exists(filekey) and overwrite:
            self.delete_file(filekey)

        fid = self.upload_file(path=filepath, name=filekey)

        if timeout > 0:
            self.rdb.set("shadow:%s" % filekey, fid)
            self.rdb.setex(filekey, timeout, fid)
        else:
            self.rdb.set(filekey, fid)

    @NotNullArgsRequired
    def delete(self, filename):
        filekey = make_weed_key(filename)
        fileid = self.rdb.get(filekey)
        if self.rdb.exists(filekey):
            self.delete_file(fileid)
            self.rdb.delete(filekey)
            return True
        return False


if __name__ == "__main__":
    pass
