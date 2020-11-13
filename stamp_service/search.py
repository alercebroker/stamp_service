import fastavro
import requests
from . import utils
import io
import boto3
from botocore.exceptions import ClientError
from urllib.request import URLopener


class S3Searcher:
    def init(self, bucket_name, client=None):
        self.client = client or boto3.client("s3")
        self.bucket_name = bucket_name

    def get_file_from_s3(self, candid):
        reverse_candid = utils.reverse_candid(candid)
        file_name = f"{reverse_candid}.avro"
        avro_file = io.BytesIO()
        try:
            # self.client.download_fileobj(self.bucket_name, file_name, avro_file)
            f = self.client.get_object(Bucket=self.bucket_name, Key=file_name)[
                "Body"
            ].read()
            avro_file = io.BytesIO(f)
            return avro_file
        except ClientError as e:
            if (
                e.response["Error"]["Code"] == "404"
                or e.response["Error"]["Code"] == "NoSuchKey"
            ):
                raise FileNotFoundError
            else:
                print()
                raise Exception(e)

    def upload_file(self, file_name, object_name):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        """
        return self.client.upload_fileobj(file_name, self.bucket_name, object_name)


class MARSSearcher:
    def init(self, mars_url):
        self.mars_url = mars_url
        self.opener = URLopener()
        # https://mars.lco.global/?candid={}&format=json

    def get_file_from_mars(self, oid, candid):
        payload = {"candid": candid, "format": "json"}
        resp = requests.get(self.mars_url, params=payload)
        resp_json = resp.json()
        self.check_response(resp_json, oid, candid)
        return resp_json["results"][0]["avro"]

    def check_response(self, resp, oid, candid):
        assert "results" in resp
        assert len(resp["results"]) == 1
        assert "objectId" in resp["results"][0]
        assert resp["results"][0]["objectId"] == oid
        assert "candid" in resp["results"][0]
        assert resp["results"][0]["candid"] == candid
        assert "avro" in resp["results"][0]


class DiscSearcher:
    def init(self, root_path, ndisk):
        self.root_path = root_path
        self.ndisk = int(ndisk)

    def get_file_from_disc(self, input_path):
        with open(input_path, "rb") as f:
            return fastavro.reader(f).next()

    def get_raw_file_from_disc(self, input_path):
        return open(input_path, "rb")


s3_searcher = S3Searcher()
mars_searcher = MARSSearcher()
disc_searcher = DiscSearcher()
