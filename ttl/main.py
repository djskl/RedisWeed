#encoding: utf-8
import redis
import threading
import gevent.select
from time import sleep
from Queue import Queue, Empty
from datetime import datetime
from redisweeds.weed import RedisWeed

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
WEEDS_HOST = "127.0.0.1"
WEEDS_PORT = 9333

EXPIRE_EVENTS = "__keyevent@0__:expired"

EXPIRE_KEYS = Queue()

class ExpireThread(threading.Thread):

    def __init__(self):
        super(ExpireThread, self).__init__()
        self.over = False

    def dismiss(self):
        self.over = True

    def run(self):
        while not self.over:
            try:
                ex_key = EXPIRE_KEYS.get(timeout=3)
            except Empty:
                continue

            conn = redis.StrictRedis(REDIS_HOST, REDIS_PORT)
            try:
                fileid = conn.get("shadow:%s"%ex_key)
            except redis.exceptions.ConnectionError:
                print "[%s]" % datetime.now().strftime("%m-%d %H:%M:%S"), "redis连接失败, 2秒后重试..."
                EXPIRE_KEYS.put(ex_key)
                sleep(2)
                continue

            if not fileid:
                continue

            rd = RedisWeed(REDIS_HOST, REDIS_PORT, WEEDS_HOST, WEEDS_PORT)
            rd.delete_file(fileid)
            conn.delete("shadow:%s"%ex_key)
            print "[%s]" % datetime.now().strftime("%m-%d %H:%M:%S"), "删除过期文件，FILEKEY: %s, FILEID: %s"%(ex_key, fileid)


def createRedisChannel(wait_timeout=1):
    try:
        conn = redis.StrictRedis(REDIS_HOST, REDIS_PORT)
        chan = conn.pubsub()
        chan.subscribe(EXPIRE_EVENTS)
        print "[%s]"%datetime.now().strftime("%m-%d %H:%M:%S"), "redis连接成功，HOST: %s, PORT: %d" %(REDIS_HOST, REDIS_PORT)
        return chan
    except redis.exceptions.ConnectionError:
        print "[%s]"%datetime.now().strftime("%m-%d %H:%M:%S"), "redis连接失败, 第%d次重新连接..."%wait_timeout
        sleep(wait_timeout)
        return createRedisChannel(wait_timeout+1)


def listenExpireEvent():
    chan = createRedisChannel()
    expire_fd = chan.connection._sock.fileno()
    while True:
        ready = gevent.select.select([expire_fd], [], [])
        if expire_fd in ready[0]:
            try:
                msg = chan.get_message()
                if msg.get("type")=="message" and msg.get("channel")=="__keyevent@0__:expired":
                    EXPIRE_KEYS.put(msg.get("data"))
            except redis.exceptions.ConnectionError:
                listenExpireEvent()


if __name__ == "__main__":
    for _ in range(3):
        ExpireThread().start()

    listenExpireEvent()