import unittest
import redis
from StringIO import StringIO
from redisweeds.base import WeedFS
from redisweeds.utils import make_weed_key
from redisweeds.weed import RedisWeed

wfs = WeedFS()
rconn = redis.StrictRedis()

class TestWeedRead(unittest.TestCase):
    def setUp(self):
        self.cnt = "123"
        self.normal_name = make_weed_key("normal_f")

        rconn.set(self.normal_name, wfs.upload_file(
            name=self.normal_name,
            stream=StringIO(self.cnt)
        ))

        self.rd = RedisWeed()

    def test_read(self):
        rst = self.rd.read(self.normal_name)
        self.assertEqual(self.cnt, rst)

if __name__ == "__main__":
    unittest.main()