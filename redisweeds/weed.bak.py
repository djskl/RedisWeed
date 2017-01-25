import io
import os
import redis
import hashlib
import StringIO
from PIL import Image

class WeedNotExists(IOError): pass
class WeedDuplicate(ValueError): pass
class WeedNoVolume(ValueError): pass



class RedisWeed(object):
    def __init__(self, redis_host="127.0.0.1", redis_port=6379, weeds_host="127.0.0.1", weeds_port=9333):
        self.rn = redis.StrictRedis(host=redis_host, port=redis_port)
        self.wn = WeedFS(master_addr=weeds_host, master_port=weeds_port)

    def get_key(self, filename):
        if len(filename) > 32:
            hb = hashlib.md5().update(filename)
            return hb.hexdigest()
        return filename

    def save_to_http(self, filename, img_type="PNG"):
        img_cnt = self.read(filename, img_type)
        if not img_cnt:
            raise WeedNotExists()
        image_file = io.BytesIO(img_cnt)
        img = Image.open(image_file)
        imgs = StringIO.StringIO()
        img.save(imgs, format=img_type)
        imgs.seek(0)
        return imgs.getvalue()

    def save_to_disk(self, filename, dst_name, img_type="PNG"):
        img_cnt = self.read(filename)
        if not img_cnt:
            raise WeedNotExists()
        image_file = io.BytesIO(img_cnt)
        img = Image.open(image_file)
        img.save(dst_name, format=img_type)

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