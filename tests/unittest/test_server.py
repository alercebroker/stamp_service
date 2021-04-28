import unittest
from unittest import mock
import os

FILE_PATH = os.path.dirname(__file__)
EXAMPLES_PATH = os.path.join(FILE_PATH, "../examples/avro_test")
os.environ["BUCKET_NAME"] = "test_bucket"
os.environ["MARS_URL"] = "test_url"
os.environ["USE_DISK"] = "True"
os.environ["ROOT_PATH"] = EXAMPLES_PATH
os.environ["NDISK"] = "1"
os.environ["TEST_MODE"] = "True"
from stamp_service.server import create_app
from stamp_service.resources import NotFound
import io


class TestStampResource(unittest.TestCase):
    def setUp(self):
        application = create_app({})
        application.config["TESTING"] = True
        with application.test_client() as client:
            self.client = client

    def tearDown(self):
        del self.client

    @mock.patch("stamp_service.search.S3Searcher.get_file_from_s3")
    @mock.patch("stamp_service.utils.get_stamp_type")
    @mock.patch("stamp_service.utils.format_stamp")
    @mock.patch("stamp_service.resources.send_file")
    @mock.patch("stamp_service.resources.fastavro.reader")
    def test_get_stamp_s3(
        self, reader, send_file, format_stamp, get_stamp_type, get_avro_from_s3
    ):
        format_stamp.return_value = (io.BytesIO, "image/png", "fname")
        send_file.return_value = "ok"
        args = {"oid": "oid", "candid": 123, "type": "science", "format": "png"}
        rv = self.client.get("/get_stamp", query_string=args)
        self.assertEqual(rv.json, "ok")

    @mock.patch("stamp_service.resources.s3_searcher.get_file_from_s3")
    @mock.patch("stamp_service.resources.disc_searcher.get_file_from_disc")
    @mock.patch("stamp_service.resources.utils.get_stamp_type")
    @mock.patch("stamp_service.resources.utils.format_stamp")
    @mock.patch("stamp_service.resources.send_file")
    def test_get_stamp_disc(
        self,
        send_file,
        format_stamp,
        get_stamp_type,
        get_file_from_disk,
        get_file_from_s3,
    ):
        get_file_from_s3.side_effect = FileNotFoundError
        format_stamp.return_value = (io.BytesIO, "image/png", "fname")
        send_file.return_value = "ok"
        args = {"oid": "oid", "candid": 123, "type": "science", "format": "png"}
        rv = self.client.get("/get_stamp", query_string=args)
        self.assertEqual(rv.json, "ok")

    @mock.patch("stamp_service.resources.s3_searcher.get_file_from_s3")
    @mock.patch("stamp_service.resources.s3_searcher.upload_file")
    @mock.patch("stamp_service.resources.disc_searcher.get_file_from_disc")
    @mock.patch("stamp_service.resources.mars_searcher.get_file_from_mars")
    @mock.patch("stamp_service.resources.utils.get_stamp_type")
    @mock.patch("stamp_service.resources.utils.format_stamp")
    @mock.patch("stamp_service.resources.send_file")
    @mock.patch("stamp_service.resources.fastavro.reader")
    def test_get_stamp_not_found(
        self,
        fastavro_reader,
        send_file,
        format_stamp,
        get_stamp_type,
        get_file_from_mars,
        get_file_from_disk,
        upload_file,
        get_file_from_s3,
    ):
        get_file_from_s3.side_effect = FileNotFoundError
        get_file_from_disk.side_effect = FileNotFoundError
        get_file_from_mars.side_effect = Exception
        format_stamp.return_value = (io.BytesIO, "image/png", "fname")
        args = {"oid": "oid", "candid": 123, "type": "science", "format": "png"}
        rv = self.client.get("/get_stamp", query_string=args)
        self.assertEqual(rv.status, "404 NOT FOUND")


class TestAVROInfoResource(unittest.TestCase):
    def setUp(self):
        application = create_app({})
        application.config["TESTING"] = True
        with application.test_client() as client:
            self.client = client

    def tearDown(self):
        del self.client

    @mock.patch("stamp_service.search.S3Searcher.get_file_from_s3")
    @mock.patch("stamp_service.resources.fastavro.reader")
    @mock.patch("stamp_service.resources.jsonify")
    def test_get_avro_s3(self, jsonify, reader, get_file_from_s3):
        get_file_from_s3.return_value = b"data"
        reader.next.return_value = {
            "cutoutScience": {},
            "cutoutTemplate": {},
            "cutoutDifference": {},
        }
        jsonify.return_value = "ok"
        args = {"oid": "oid", "candid": 123, "type": "science", "format": "png"}
        rv = self.client.get("/get_avro_info", query_string=args)
        self.assertEqual(rv.json, "ok")

    @mock.patch("stamp_service.resources.s3_searcher.get_file_from_s3")
    @mock.patch("stamp_service.resources.disc_searcher.get_file_from_disc")
    @mock.patch("stamp_service.resources.fastavro.reader")
    @mock.patch("stamp_service.resources.jsonify")
    def test_get_stamp_disc(
        self,
        jsonify,
        reader,
        get_file_from_disc,
        get_file_from_s3,
    ):
        get_file_from_s3.side_effect = FileNotFoundError
        args = {"oid": "oid", "candid": 123, "type": "science", "format": "png"}
        reader.next.return_value = {
            "cutoutScience": {},
            "cutoutTemplate": {},
            "cutoutDifference": {},
        }
        jsonify.return_value = "ok"
        rv = self.client.get("/get_avro_info", query_string=args)
        self.assertEqual(rv.json, "ok")

    @mock.patch("stamp_service.resources.s3_searcher.get_file_from_s3")
    @mock.patch("stamp_service.resources.s3_searcher.upload_file")
    @mock.patch("stamp_service.resources.disc_searcher.get_file_from_disc")
    @mock.patch("stamp_service.resources.mars_searcher.get_file_from_mars")
    @mock.patch("stamp_service.resources.fastavro.reader")
    @mock.patch("stamp_service.resources.jsonify")
    @mock.patch("stamp_service.resources.mars_searcher.opener.open")
    def test_get_stamp_mars(
        self,
        opener_open,
        jsonify,
        reader,
        get_file_from_mars,
        get_file_from_disk,
        upload_file,
        get_file_from_s3,
    ):
        get_file_from_s3.side_effect = FileNotFoundError
        get_file_from_disk.side_effect = FileNotFoundError
        get_file_from_mars.return_value = "test"
        opener_open.return_value = b"data"
        reader.next.return_value = {
            "cutoutScience": {},
            "cutoutTemplate": {},
            "cutoutDifference": {},
        }
        jsonify.return_value = "ok"
        args = {"oid": "oid", "candid": 123, "type": "science", "format": "png"}
        rv = self.client.get("/get_avro_info", query_string=args)
        self.assertEqual(rv.json, "ok")

    @mock.patch("stamp_service.resources.s3_searcher.get_file_from_s3")
    @mock.patch("stamp_service.resources.s3_searcher.upload_file")
    @mock.patch("stamp_service.resources.disc_searcher.get_file_from_disc")
    @mock.patch("stamp_service.resources.mars_searcher.get_file_from_mars")
    @mock.patch("stamp_service.resources.fastavro.reader")
    def test_get_stamp_not_found(
        self,
        fastavro_reader,
        get_file_from_mars,
        get_file_from_disk,
        upload_file,
        get_file_from_s3,
    ):
        get_file_from_s3.side_effect = FileNotFoundError
        get_file_from_disk.side_effect = FileNotFoundError
        get_file_from_mars.side_effect = Exception
        args = {"oid": "oid", "candid": 123, "type": "science", "format": "png"}
        rv = self.client.get("/get_avro_info", query_string=args)
        self.assertEqual(rv.status, "404 NOT FOUND")


class TestPutAVROResource(unittest.TestCase):
    def setUp(self):
        application = create_app({})
        application.config["TESTING"] = True
        with application.test_client() as client:
            self.client = client

    def tearDown(self):
        del self.client

    @mock.patch("stamp_service.resources.s3_searcher.upload_file")
    @mock.patch("stamp_service.resources.jsonify")
    def test_post(self, jsonify, upload_file):
        jsonify.return_value = "ok"
        rv = self.client.post(
            "/put_avro",
            data={"avro": (io.BytesIO(b"data"), "avro.avro"), "candid": 123},
            follow_redirects=True,
            content_type="multipart/form-data",
        )
        self.assertEqual(rv.json, "ok")


class TestAVROResource(unittest.TestCase):
    def setUp(self):
        application = create_app({})
        application.config["TESTING"] = True
        with application.test_client() as client:
            self.client = client

    def tearDown(self):
        del self.client

    @mock.patch("stamp_service.search.S3Searcher.get_file_from_s3")
    @mock.patch("stamp_service.resources.send_file")
    def test_get_avro_s3(self, send_file, get_file_from_s3):
        get_file_from_s3.return_value = b"data"
        send_file.return_value = "ok"
        args = {"oid": "oid", "candid": 123, "type": "science", "format": "png"}
        rv = self.client.get("/get_avro", query_string=args)
        self.assertEqual(rv.json, "ok")

    @mock.patch("stamp_service.resources.s3_searcher.get_file_from_s3")
    @mock.patch("stamp_service.resources.disc_searcher.get_raw_file_from_disc")
    @mock.patch("stamp_service.resources.send_file")
    def test_get_stamp_disc(
        self,
        send_file,
        get_file_from_disc,
        get_file_from_s3,
    ):
        get_file_from_s3.side_effect = FileNotFoundError
        args = {"oid": "oid", "candid": 123, "type": "science", "format": "png"}
        send_file.return_value = "ok"
        rv = self.client.get("/get_avro", query_string=args)
        self.assertEqual(rv.json, "ok")

    @mock.patch("stamp_service.resources.s3_searcher.get_file_from_s3")
    @mock.patch("stamp_service.resources.s3_searcher.upload_file")
    @mock.patch("stamp_service.resources.disc_searcher.get_file_from_disc")
    @mock.patch("stamp_service.resources.mars_searcher.get_file_from_mars")
    @mock.patch("stamp_service.resources.send_file")
    @mock.patch("stamp_service.resources.mars_searcher.opener.open")
    def test_get_stamp_mars(
        self,
        opener_open,
        send_file,
        get_file_from_mars,
        get_file_from_disk,
        upload_file,
        get_file_from_s3,
    ):
        opener_open.return_value = b"data"
        get_file_from_s3.side_effect = FileNotFoundError
        get_file_from_disk.side_effect = FileNotFoundError
        get_file_from_mars.return_value = "test"
        send_file.return_value = "ok"
        args = {"oid": "oid", "candid": 123, "type": "science", "format": "png"}
        rv = self.client.get("/get_avro", query_string=args)
        self.assertEqual(rv.json, "ok")

    @mock.patch("stamp_service.resources.s3_searcher.get_file_from_s3")
    @mock.patch("stamp_service.resources.s3_searcher.upload_file")
    @mock.patch("stamp_service.resources.disc_searcher.get_file_from_disc")
    @mock.patch("stamp_service.resources.mars_searcher.get_file_from_mars")
    def test_get_stamp_not_found(
        self,
        get_file_from_mars,
        get_file_from_disk,
        upload_file,
        get_file_from_s3,
    ):
        get_file_from_s3.side_effect = FileNotFoundError
        get_file_from_disk.side_effect = FileNotFoundError
        get_file_from_mars.side_effect = Exception
        args = {"oid": "oid", "candid": 123, "type": "science", "format": "png"}
        rv = self.client.get("/get_avro", query_string=args)
        self.assertEqual(rv.status, "404 NOT FOUND")
