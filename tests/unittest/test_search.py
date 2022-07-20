import unittest
from unittest import mock
from stamp_service.search import S3Searcher, MARSSearcher, boto3, io
from moto import mock_s3
import os

TEST_BUCKET_CONFIG = {
    'ztf': {
        'id': "ztf",
        'bucket': "test_bucket"
    }
}

@mock_s3
class TestS3Searcher(unittest.TestCase):
    def setUp(self):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        # self.conn = boto3.resource("s3", region_name="us-east-1")
        # self.conn.create_bucket(Bucket="test_bucket")
        self.client = boto3.client("s3")
        self.client.create_bucket(Bucket="test_bucket")
        self.upload_file("test_bucket")
        self.searcher = S3Searcher()
        self.searcher.init(bucket_config=TEST_BUCKET_CONFIG, client=self.client)

    def tearDown(self):
        del self.client
        del self.searcher

    def upload_file(self, bucket):
        file_path = os.path.dirname(__file__)
        avro_path = os.path.join(
            file_path, "../examples/avro_test/ZTF18/a/c/u/w/w/p/p/"
        )
        candid_str = "820128985515010010"
        object_name = f"{candid_str}.avro"
        reversed = candid_str[::-1]
        object_name_reversed = f"{reversed}.avro"
        avro_path += f"/{object_name}"
        with open(avro_path, "rb") as avro:
            self.client.upload_fileobj(avro, bucket, object_name_reversed)

    def test_get_file_from_s3(self):
        candid = "820128985515010010"
        file = self.searcher.get_file_from_s3(candid, 'ztf')
        self.assertIsInstance(file, io.BytesIO)

    def test_upload_file(self):
        file = io.BytesIO()
        self.assertEqual(
            len(self.client.list_objects(Bucket="test_bucket")["Contents"]), 1
        )
        self.searcher.upload_file(file, object_name="fake_object", survey_id='ztf')
        self.assertEqual(
            len(self.client.list_objects(Bucket="test_bucket")["Contents"]), 2
        )


class TestMARSSearcher(unittest.TestCase):
    def setUp(self):
        self.searcher = MARSSearcher()
        self.searcher.init(mars_url="fake_url")

    def tearDown(self):
        del self.searcher

    @mock.patch("requests.get")
    @mock.patch.object(MARSSearcher, "check_response")
    @mock.patch("stamp_service.search.urlopen")
    def test_get_file_from_mars(self, mock_open, mock_check_response, mock_get):
        mock_get.return_value.json.return_value = {"results": [{"avro": "avro"}]}
        mock_open.return_value.__enter__.return_value.read.return_value = b"avro"
        mock_get.return_value.status_code = 200
        resp = self.searcher.get_file_from_mars("oid", 123)
        self.assertIsInstance(resp, io.BytesIO)

    def test_check_response(self):
        resp = {
            "results": [{"objectId": "oid", "candid": 123, "avro": "avro"}],
        }
        self.searcher.check_response(resp, "oid", 123)

    def test_check_response_raises(self):
        resp = {}
        resp2 = {
            "results": [{"objectId": "oid", "candid": 123, "avro": "avro"}, "other"],
        }
        resp3 = {
            "results": [{"candid": 123, "avro": "avro"}],
        }
        resp4 = {
            "results": [{"objectId": "something", "candid": 123, "avro": "avro"}],
        }
        resp5 = {
            "results": [{"objectId": "something", "avro": "avro"}],
        }
        resp6 = {
            "results": [{"objectId": "something", "candid": 789, "avro": "avro"}],
        }
        resp7 = {
            "results": [{"objectId": "something", "candid": 789}],
        }
        with self.assertRaises(AssertionError) as context:
            self.searcher.check_response(resp, "oid", 123)
        with self.assertRaises(AssertionError) as context:
            self.searcher.check_response(resp2, "oid", 123)
        with self.assertRaises(AssertionError) as context:
            self.searcher.check_response(resp3, "oid", 123)
        with self.assertRaises(AssertionError) as context:
            self.searcher.check_response(resp4, "oid", 123)
        with self.assertRaises(AssertionError) as context:
            self.searcher.check_response(resp5, "oid", 123)
        with self.assertRaises(AssertionError) as context:
            self.searcher.check_response(resp6, "oid", 123)
        with self.assertRaises(AssertionError) as context:
            self.searcher.check_response(resp7, "oid", 123)
