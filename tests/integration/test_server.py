from vcr_unittest import VCRTestCase
from unittest import mock
from moto import mock_s3
import os
import io

FILE_PATH = os.path.dirname(__file__)
EXAMPLES_PATH = os.path.join(FILE_PATH, "../examples/avro_test")
os.environ["BUCKET_NAME"] = "test_bucket"
os.environ["MARS_URL"] = "https://mars.lco.global/"
os.environ["USE_DISK"] = "True"
os.environ["ROOT_PATH"] = EXAMPLES_PATH
os.environ["NDISK"] = "1"
os.environ["TEST_MODE"] = "True"
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
from stamp_service.server import create_app
from stamp_service.resources import NotFound
import boto3


@mock_s3
class TestStampResource(VCRTestCase):
    def setUp(self):
        super().setUp()
        self.application = create_app({})
        with self.application.test_client() as client:
            self.application.config["TESTING"] = True
            self.test_client = client
            self.conn = boto3.resource("s3")
            self.conn.create_bucket(Bucket="test_bucket")

    def tearDown(self):
        super().tearDown()
        self.conn.Bucket("test_bucket").objects.all().delete()
        del self.conn
        del self.test_client
        del self.application

    def upload_file(self, bucket):
        avro_path = os.path.join(EXAMPLES_PATH, "ZTF18/a/c/u/w/w/p/p/")
        candid_str = "820128985515010010"
        object_name = f"{candid_str}.avro"
        reversed = candid_str[::-1]
        object_name_reversed = f"{reversed}.avro"
        avro_path += f"/{object_name}"
        with open(avro_path, "rb") as avro:
            client = boto3.client("s3")
            client.upload_fileobj(avro, bucket, object_name_reversed)

    def test_get_stamp_s3(self):
        self.upload_file("test_bucket")
        args = {
            "oid": "ZTF18acuwwpp",
            "candid": 820128985515010010,
            "type": "science",
            "format": "png",
        }
        rv = self.test_client.get("/get_stamp", query_string=args)
        self.assertEqual(rv.status, "200 OK")

    def test_get_stamp_disc(self):
        args = {
            "oid": "ZTF18acuwwpp",
            "candid": 820128985515010010,
            "type": "science",
            "format": "png",
        }
        rv = self.test_client.get("/get_stamp", query_string=args)
        self.assertEqual(rv.status, "200 OK")

    @mock.patch("stamp_service.resources.disc_searcher.get_file_from_disc")
    def test_get_stamp_mars(self, get_file_from_disc):
        get_file_from_disc.side_effect = FileNotFoundError
        client = boto3.client("s3")
        args = {
            "oid": "ZTF18acuwwpp",
            "candid": 820128985515010010,
            "type": "science",
            "format": "png",
        }
        objs = client.list_objects(Bucket="test_bucket")
        self.assertNotIn("Contents", objs)
        rv = self.test_client.get("/get_stamp", query_string=args)
        objs = client.list_objects(Bucket="test_bucket")
        self.assertEqual(len(objs["Contents"]), 1)
        self.assertEqual(rv.status, "200 OK")
        self.assertEqual(len(self.cassette), 2)

    def test_get_avro_not_found(self):
        args = {
            "oid": "ZTF18acuwwpp",
            "candid": 123,
            "type": "science",
            "format": "png",
        }
        rv = self.test_client.get("/get_stamp", query_string=args)
        self.assertEqual(rv.status, "404 NOT FOUND")


@mock_s3
class TestAvroInfoResource(VCRTestCase):
    def setUp(self):
        super().setUp()
        self.application = create_app({})
        with self.application.test_client() as client:
            self.application.config["TESTING"] = True
            self.test_client = client
            self.conn = boto3.resource("s3")
            self.conn.create_bucket(Bucket="test_bucket")

    def tearDown(self):
        super().tearDown()
        self.conn.Bucket("test_bucket").objects.all().delete()
        del self.conn
        del self.test_client
        del self.application

    def upload_file(self, bucket):
        avro_path = os.path.join(EXAMPLES_PATH, "ZTF18/a/c/u/w/w/p/p/")
        candid_str = "820128985515010010"
        object_name = f"{candid_str}.avro"
        reversed = candid_str[::-1]
        object_name_reversed = f"{reversed}.avro"
        avro_path += f"/{object_name}"
        with open(avro_path, "rb") as avro:
            client = boto3.client("s3")
            client.upload_fileobj(avro, bucket, object_name_reversed)

    def test_get_avro_info_s3(self):
        self.upload_file("test_bucket")
        args = {
            "oid": "ZTF18acuwwpp",
            "candid": 820128985515010010,
            "type": "science",
            "format": "png",
        }
        rv = self.test_client.get("/get_avro_info", query_string=args)
        self.assertEqual(rv.status, "200 OK")


    def test_get_avro_info_disc(self):
        args = {
            "oid": "ZTF18acuwwpp",
            "candid": 820128985515010010,
            "type": "science",
            "format": "png",
        }
        rv = self.test_client.get("/get_avro_info", query_string=args)
        self.assertEqual(rv.status, "200 OK")

    @mock.patch("stamp_service.resources.disc_searcher.get_file_from_disc")
    def test_get_avro_info_mars(self, get_file_from_disc):
        get_file_from_disc.side_effect = FileNotFoundError
        client = boto3.client("s3")
        args = {
            "oid": "ZTF18acuwwpp",
            "candid": 820128985515010010,
            "type": "science",
            "format": "png",
        }
        objs = client.list_objects(Bucket="test_bucket")
        self.assertNotIn("Contents", objs)
        rv = self.test_client.get("/get_avro_info", query_string=args)
        objs = client.list_objects(Bucket="test_bucket")
        self.assertEqual(len(objs["Contents"]), 1)
        self.assertEqual(rv.status, "200 OK")
        self.assertEqual(len(self.cassette), 2)

    def test_get_avro_not_found(self):
        args = {
            "oid": "ZTF18acuwwpp",
            "candid": 123,
            "type": "science",
            "format": "png",
        }
        rv = self.test_client.get("/get_avro_info", query_string=args)
        self.assertEqual(rv.status, "404 NOT FOUND")


@mock_s3
class TestPutAvroResource(VCRTestCase):
    def setUp(self):
        super().setUp()
        self.application = create_app({})
        with self.application.test_client() as client:
            self.application.config["TESTING"] = True
            self.test_client = client
            self.conn = boto3.resource("s3")
            self.conn.create_bucket(Bucket="test_bucket")

    def tearDown(self):
        super().tearDown()
        self.conn.Bucket("test_bucket").objects.all().delete()
        del self.conn
        del self.test_client
        del self.application

    def test_post(self):
        client = boto3.client("s3")
        objs = client.list_objects(Bucket="test_bucket")
        self.assertNotIn("Contents", objs)
        rv = self.test_client.post(
            "/put_avro",
            data={"avro": (io.BytesIO(b"data"), "avro.avro"), "candid": 123},
            follow_redirects=True,
            content_type="multipart/form-data"
        )
        objs = client.list_objects(Bucket="test_bucket")
        self.assertEqual(rv.status, "200 OK")
        self.assertEqual(len(objs["Contents"]), 1)


@mock_s3
class TestAvroResource(VCRTestCase):
    def setUp(self):
        super().setUp()
        self.application = create_app({})
        with self.application.test_client() as client:
            self.application.config["TESTING"] = True
            self.test_client = client
            self.conn = boto3.resource("s3")
            self.conn.create_bucket(Bucket="test_bucket")

    def tearDown(self):
        super().tearDown()
        self.conn.Bucket("test_bucket").objects.all().delete()
        del self.conn
        del self.test_client
        del self.application

    def upload_file(self, bucket):
        avro_path = os.path.join(EXAMPLES_PATH, "ZTF18/a/c/u/w/w/p/p/")
        candid_str = "820128985515010010"
        object_name = f"{candid_str}.avro"
        reversed = candid_str[::-1]
        object_name_reversed = f"{reversed}.avro"
        avro_path += f"/{object_name}"
        with open(avro_path, "rb") as avro:
            client = boto3.client("s3")
            client.upload_fileobj(avro, bucket, object_name_reversed)

    def test_get_avro_s3(self):
        self.upload_file("test_bucket")
        args = {
            "oid": "ZTF18acuwwpp",
            "candid": 820128985515010010,
            "type": "science",
            "format": "png",
        }
        rv = self.test_client.get("/get_avro", query_string=args)
        io.BytesIO(rv.data)
        self.assertEqual(rv.status, "200 OK")

    def test_get_avro_disc(self):
        args = {
            "oid": "ZTF18acuwwpp",
            "candid": 820128985515010010,
            "type": "science",
            "format": "png",
        }
        rv = self.test_client.get("/get_avro", query_string=args)
        io.BytesIO(rv.data)
        self.assertEqual(rv.status, "200 OK")

    @mock.patch("stamp_service.resources.disc_searcher.get_raw_file_from_disc")
    def test_get_avro_mars(self, get_file_from_disc):
        get_file_from_disc.side_effect = FileNotFoundError
        client = boto3.client("s3")
        args = {
            "oid": "ZTF18acuwwpp",
            "candid": 820128985515010010,
            "type": "science",
            "format": "png",
        }
        objs = client.list_objects(Bucket="test_bucket")
        self.assertNotIn("Contents", objs)
        rv = self.test_client.get("/get_avro", query_string=args)
        objs = client.list_objects(Bucket="test_bucket")
        self.assertEqual(len(objs["Contents"]), 1)
        self.assertEqual(rv.status, "200 OK")
        self.assertEqual(len(self.cassette), 2)

    def test_get_avro_not_found(self):
        args = {
            "oid": "ZTF18acuwwpp",
            "candid": 123,
            "type": "science",
            "format": "png",
        }
        rv = self.test_client.get("/get_avro", query_string=args)
        self.assertEqual(rv.status, "404 NOT FOUND")


