# -*- coding: utf-8 -*-

"""This is helper module that contains functions
to easeup communication with weed-fs
"""
import redis
import hashlib
import requests
from redisweeds.version import __version__

def make_weed_key(filename):
    if len(filename) > 32:
        hb = hashlib.md5().update(filename)
        return hb.hexdigest()
    return filename

def redis_connect(redis_host="localhost", redis_port=6379):
    _rdb = redis.StrictRedis(host=redis_host, port=redis_port)
    try:
        is_alive = _rdb.ping()
    except redis.exceptions.ConnectionError:
        return None

    if is_alive:
        return _rdb
    return None

class Connection(object):

    def __init__(self, use_session=False):
        if use_session:
            self._conn = requests.Session()
        else:
            self._conn = requests

    def _prepare_headers(self, additional_headers=None, **kwargs):
        """Prepare headers for http communication.

        Return dict of header to be used in requests.

        Args:
            .. versionadded:: 0.3.2
                **additional_headers**: (optional) Additional headers
                to be used with request

        Returns:
            Headers dict. Key and values are string

        """
        user_agent = "pyseaweed/{version}".format(version=__version__)
        headers = {"User-Agent": user_agent}
        if additional_headers is not None:
            headers.update(additional_headers)
        return headers

    def head(self, url, *args, **kwargs):
        """Returns response to http HEAD
        on provided url
        """
        res = self._conn.head(url, headers=self._prepare_headers(**kwargs))
        if res.status_code == 200:
            return res
        return None

    def get_data(self, url, params=None, **kwargs):
        """Gets data from url as text

        Returns content under the provided url as text

        Args:
            **url**: address of the wanted data

            .. versionadded:: 0.3.2
                **additional_headers**: (optional) Additional headers
                to be used with request

        Returns:
            string

        """
        res = self._conn.get(url, params=params, headers=self._prepare_headers(**kwargs))
        if res.status_code == 200:
            return res.text
        else:
            return None

    def get_raw_data(self, url, *args, **kwargs):
        """Gets data from url as bytes

        Returns content under the provided url as bytes
        ie. for binary data

        Args:
            **url**: address of the wanted data

            .. versionadded:: 0.3.2
                **additional_headers**: (optional) Additional headers
                to be used with request

        Returns:
            bytes

        """
        res = self._conn.get(url, headers=self._prepare_headers(**kwargs))
        if res.status_code == 200:
            return res.content
        else:
            return None

    def post_file(self, url, filename, file_stream, *args, **kwargs):
        """Uploads file to provided url.

        Returns contents as text

        Args:
            **url**: address where to upload file

            **filename**: Name of the uploaded file

            **file_stream**: file like object to upload

            .. versionadded:: 0.3.2
                **additional_headers**: (optional) Additional headers
                to be used with request

        Returns:
            string
        """
        res = self._conn.post(url, files={filename: file_stream},
                              headers=self._prepare_headers(**kwargs))
        if res.status_code == 200 or res.status_code == 201:
            return res.text
        else:
            return None

    def delete_data(self, url, *args, **kwargs):
        """Deletes data under provided url

        Returns status as boolean.

        Args:
            **url**: address of file to be deleted

            .. versionadded:: 0.3.2
                **additional_headers**: (optional) Additional headers
                to be used with request

        Returns:
            Boolean. True if request was successful. False if not.
        """
        res = self._conn.delete(url, headers=self._prepare_headers(**kwargs))
        if res.status_code == 200 or res.status_code == 202:
            return True
        else:
            return False


# def save_to_http(self, filename, img_type="PNG"):
#     img_cnt = self.read(filename, img_type)
#     if not img_cnt:
#         raise WeedNotExists()
#     image_file = io.BytesIO(img_cnt)
#     img = Image.open(image_file)
#     imgs = StringIO.StringIO()
#     img.save(imgs, format=img_type)
#     imgs.seek(0)
#     return imgs.getvalue()
#
#
# def save_to_disk(self, filename, dst_name, img_type="PNG"):
#     img_cnt = self.read(filename)
#     if not img_cnt:
#         raise WeedNotExists()
#     image_file = io.BytesIO(img_cnt)
#     img = Image.open(image_file)
#     img.save(dst_name, format=img_type)