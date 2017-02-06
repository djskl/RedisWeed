from redisweeds.weed import RedisWeed
import random

class Test(object):

    def __init__(self):
        self.rd = RedisWeed(redis_host="10.0.83.91", master_addr="10.0.83.159")
        self.test_filename = "test_timeout_filename"
        self.test_cnt = "ces"

    def write(self):
        for idx in range(10):
            self.rd.write("_".join([self.test_filename, str(idx)]), self.test_cnt, timeout=random.randint(1, 10))

if __name__ == "__main__":
    t = Test()
    t.write()
