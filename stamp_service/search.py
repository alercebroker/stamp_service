import requests
from . import utils
import io
import boto3
from botocore.exceptions import ClientError
from urllib.request import urlopen


class S3Searcher:
    def init(self, bucket_config, client=None):
        self.client = client or boto3.client("s3")
        self.buckets_dict = bucket_config

    def get_file_from_s3(self, candid, survey_id):
        reverse_candid = utils.reverse_candid(candid)
        file_name = f"{reverse_candid}.avro"
        bucket_name = self.buckets_dict[survey_id]["bucket"]
        try:
            # self.client.download_fileobj(self.bucket_name, file_name, avro_file)
            f = self.client.get_object(Bucket=bucket_name, Key=file_name)["Body"].read()
            avro_file = io.BytesIO(f)
            return avro_file
        except ClientError as e:
            if (
                e.response["Error"]["Code"] == "404"
                or e.response["Error"]["Code"] == "NoSuchKey"
            ):
                raise FileNotFoundError
            else:
                raise Exception(e)

    def upload_file(self, file_name, object_name, survey_id):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        """
        bucket_name = self.buckets_dict[survey_id]["bucket"]
        return self.client.upload_fileobj(file_name, bucket_name, object_name)


class MARSSearcher:
    def init(self, mars_url):
        self.mars_url = mars_url

    def get_file_from_mars(self, oid, candid):
        payload = {"candid": int(candid), "format": "json"}
        resp = requests.get(self.mars_url, params=payload)
        if resp.status_code != 200:
            raise Exception("Unable to download from MARS")
        resp_json = resp.json()
        self.check_response(resp_json, oid, candid)
        with urlopen(resp_json["results"][0]["avro"]) as f:
            return io.BytesIO(f.read())

    def check_response(self, resp, oid, candid):
        assert "results" in resp
        assert len(resp["results"]) == 1
        assert "objectId" in resp["results"][0]
        if oid:
            assert resp["results"][0]["objectId"] == oid
        assert "candid" in resp["results"][0]
        assert resp["results"][0]["candid"] == candid
        assert "avro" in resp["results"][0]
