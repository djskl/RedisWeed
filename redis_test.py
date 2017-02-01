import unittest
import redis
from redisweeds.base import WeedFS
from redisweeds.exceptions import FileNotExists, WeedDuplicateError
from redisweeds.utils import make_weed_key
from redisweeds.weed import RedisWeed

wfs = WeedFS()
rconn = redis.StrictRedis()


class TestWeedRead(unittest.TestCase):

    def setUp(self):
        self.rd = RedisWeed()
        self.test_filename = "test_read_filename"
        self.test_filekey = make_weed_key(self.test_filename)
        self.test_cnt = "ces"

        self.rd.write(self.test_filename, self.test_cnt)

    def tearDown(self):
        wfs.delete_file(rconn.get(self.test_filekey))
        rconn.delete(self.test_filekey)

    def test_read(self):
        ret = self.rd.read(self.test_filename)
        self.assertEqual(self.test_cnt, ret)

    def test_read_no_exists(self):
        with self.assertRaises(FileNotExists) as cm:
            self.rd.read("no_exists_filename")

        the_exception = cm.exception
        self.assertEqual(the_exception.filename, "no_exists_filename")

class TestWeedWrite(unittest.TestCase):

    def setUp(self):
        self.rd = RedisWeed()
        self.test_filename = "test_write_filename"
        self.test_filekey = make_weed_key(self.test_filename)
        self.test_cnt_1 = "ces1"
        self.test_cnt_2 = "ces2"

    def tearDown(self):
        wfs.delete_file(rconn.get(self.test_filekey))
        rconn.delete(self.test_filekey)

    def test_write(self):
        self.rd.write(self.test_filename, self.test_cnt_1)

        self.assertTrue(rconn.exists(self.test_filekey))

        ret = wfs.get_file(rconn.get(self.test_filekey))
        self.assertEqual(self.test_cnt_1, ret)

    def test_no_overwrite(self):
        self.rd.write(self.test_filename, self.test_cnt_1)

        with self.assertRaises(WeedDuplicateError):
            self.rd.write(self.test_filename, self.test_cnt_2)

    def test_overwrite(self):

        self.rd.write(self.test_filename, self.test_cnt_1)
        self.rd.write(self.test_filename, self.test_cnt_2, True)

        ret = wfs.get_file(rconn.get(self.test_filekey))
        self.assertEqual(self.test_cnt_2, ret)


class TestWeedDelete(unittest.TestCase):

    def setUp(self):
        self.rd = RedisWeed()
        self.test_filename = "test_delete_filename"
        self.test_filekey = make_weed_key(self.test_filename)
        self.test_cnt = "ces"

        self.rd.write(self.test_filename, self.test_cnt)

    def test_delete(self):
        self.assertTrue(rconn.exists(self.test_filekey))
        fileid = rconn.get(self.test_filekey)
        self.assertTrue(wfs.file_exists(fileid))

        ret = self.rd.delete(self.test_filename)

        self.assertTrue(ret)

        self.assertFalse(rconn.exists(self.test_filekey))
        self.assertFalse(wfs.file_exists(fileid))


if __name__ == "__main__":
    unittest.main()