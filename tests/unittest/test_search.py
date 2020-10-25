import unittest
from unittest import mock
from stamp_service.search import S3Searcher, DiscSearcher, MARSSearcher, boto3, io
from moto import mock_s3
import os


class TestS3Searcher(unittest.TestCase):
    @mock_s3
    def setUp(self):
        """Mocked AWS Credentials for moto."""
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        os.environ['AWS_SECURITY_TOKEN'] = 'testing'
        os.environ['AWS_SESSION_TOKEN'] = 'testing'
        self.client = boto3.resource("s3", region_name="us-east-1")
        self.client.create_bucket(Bucket="test_bucket")
        self.searcher = S3Searcher(bucket_name="test_bucket")

    def tearDown(self):
        del self.client
        del self.searcher

    @mock.patch("stamp_service.utils.reverse_candid")
    def test_get_file_from_s3(self, mock_reverse_candid):
        oid = "oid"
        candid = 123
        file = self.searcher.get_file_from_s3(oid, candid)
        self.assertIsInstance(file, io.BytesIO)
