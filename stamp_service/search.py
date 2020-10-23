import fastavro
import requests


class S3Searcher:
    def get_file_from_s3(self, oid, candid):
        pass


class MARSSearcher:
    def __init__(self, mars_url):
        self.mars_url = mars_url

    def get_file_from_mars(self, oid, candid):
        payload = {"candid": candid, "format": "json"}
        resp = requests.get(self.mars_url, params=payload)
        resp_json = resp.json()
        check_response(resp_json, oid, candid)
        return resp_json["results"][0]["avro"]

    def check_response(self, resp, oid, candid):
        assert "results" in resp
        assert len(resp) == 1
        assert "objectId" in resp["results"][0]
        assert resp["results"][0]["objectId"] == oid
        assert "candid" in resp["results"][0]
        assert resp["results"][0]["candid"] == candid
        assert "avro" in resp["results"][0]


class DiscSearcher:
    def get_file_from_disc(self, input_path):
        with open(input_path, "rb") as f:
            return fastavro.reader(f).next()
