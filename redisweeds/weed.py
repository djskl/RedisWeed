from redisweeds.base import WeedFS
from redisweeds.constants import REDIS_VOLUME_KEY
from redisweeds.exceptions import RedisConnError
from redisweeds.utils import redis_connect


class RedisWeed(WeedFS):
    def __init__(self, redis_host="localhost", redis_port=6379,
                 master_addr='localhost', master_port=9333, use_session=False):
        super(self, RedisWeed).__init__(master_addr, master_port, use_session)
        self.rdb = redis_connect(redis_host, redis_port)
        if not self.rdb:
            raise RedisConnError(redis_port, redis_port)
        self.load_alive_volumes(self.rdb)

    def load_alive_volumes(self, redis_conn):
        vols = self.get_all_volumes()
        if not vols:
            return
        redis_conn.sadd(REDIS_VOLUME_KEY, *vols)

    def get_volumeId(self):
        while self.rn.scard(REDIS_VOLUME_KEY) > 0:
            vid = self.rn.srandmember(REDIS_VOLUME_KEY)
            isAlive = self.wn.check_volume_status(vid)
            if isAlive:
                return vid
            self.rn.srem(REDIS_VOLUME_KEY, vid)

        return None


    def read(self, filename):
        vid = self.get_volumeId()
        if not vid:
            raise WeedNoVolume()

        filekey = self.rn.get(filename)
        if not filekey:
            return None

        img_cnt = self.wn.get_file(",".join([vid, filekey]))
        return img_cnt


    def write(self, filename, img, timeout=0):
        filekey = self.get_key(filename)
        if self.rn.exists(filekey):
            raise WeedDuplicate()

        imgs = StringIO.StringIO()
        img.save(imgs, format='PNG')
        imgs.seek(0)

        filekey = self.get_key(filename)
        if timeout > 0:
            fid = self.wn.upload_file(name=filekey, stream=imgs, ttl=timeout)
        else:
            fid = self.wn.upload_file(name=filekey, stream=imgs)

        vid, key = fid.split(",")
        self.rn.sadd(REDIS_VOLUME_KEY, vid)
        if timeout > 0:
            self.rn.set(filekey, key, ex=timeout * 60)


    def upload(self, filename, filepath, timeout=0):
        if not os.path.exists(filepath):
            raise IOError()

        filekey = self.get_key(filename)
        if self.rn.exists(filekey):
            raise WeedDuplicate()

        if timeout > 0:
            fid = self.wn.upload_file(path=filepath, name=filekey, ttl=timeout)
        else:
            fid = self.wn.upload_file(path=filepath, name=filekey)

        vid, key = fid.split(",")
        self.rn.sadd(REDIS_VOLUME_KEY, vid)
        if timeout > 0:
            self.rn.set(filekey, key, ex=timeout * 60)
        else:
            self.rn.set(filekey, key)


if __name__ == "__main__":
    rw = RedisWeed(redis_host="10.0.83.47", weeds_host="10.0.83.159")
    # rw.upload("ces2", "/root/Pictures/p1.png")
    rw.save_to_disk("ces2", "/tmp/hell.png")