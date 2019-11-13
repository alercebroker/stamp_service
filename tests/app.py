import unittest
import requests
import os

class TestStampService(unittest.TestCase):

    def test_get_stamp(self):
        oid =  "ZTF18aaccpyz"
        candid = "516167660315010003"
        format = "png"
        type = "science"

        params = {
            "oid":oid,
            "candid":candid,
            "format":format,
            "type":type
        }


        resp = requests.get("http://localhost:8087/get_stamp",params=params)
        self.assertEqual(resp.status_code,200)
        self.assertEqual(resp.headers["Content-Type"], 'image/png')

        params["format"] = "fits"
        resp = requests.get("http://localhost:8087/get_stamp",params=params)
        self.assertEqual(resp.status_code,200)
        self.assertEqual(resp.headers["Content-Type"], 'application/fits+gzip')


    def test_get_avro_info(self):
        oid =  "ZTF18aaccpyz"
        candid = "516167660315010003"

        params = {
            "oid":oid,
            "candid":candid,
        }
        resp = requests.get("http://localhost:8087/get_avro_info",params=params)
        self.assertEqual(resp.status_code,200)
        self.assertEqual(type(resp.json()), dict )

    def test_wrong_oid(self):
        oid =  "TESTO"
        candid = "516167660315010003"
        format = "png"
        type = "science"

        params = {
            "oid":oid,
            "candid":candid,
            "format":format,
            "type":type
        }

        resp = requests.get("http://localhost:8087/get_stamp",params=params)
        self.assertEqual(resp.status_code,500)

    def test_wrong_candic(self):
        oid =  "ZTF18aaccpyz"
        candid = 0
        format = "png"
        type = "science"

        params = {
            "oid":oid,
            "candid":candid,
            "format":format,
            "type":type
        }

        resp = requests.get("http://localhost:8087/get_stamp",params=params)
        self.assertEqual(resp.status_code,500)



if __name__ == '__main__':
    unittest.main()
