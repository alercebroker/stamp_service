import unittest
from unittest import mock
from stamp_service import utils
import os


class TestUtils(unittest.TestCase):
    def test_reverse_candid(self):
        n = utils.reverse_candid(123456)
        self.assertEqual(n, "654321")
        n = utils.reverse_candid("123456")
        self.assertEqual(n, "654321")

    def test_get_stamp_type(self):
        avro = {
            "cutoutScience": {"stampData": "science"},
            "cutoutTemplate": {"stampData": "template"},
            "cutoutDifference": {"stampData": "difference"},
        }
        type = utils.get_stamp_type(avro, "science")
        self.assertEqual(type, "science")
        type = utils.get_stamp_type(avro, "template")
        self.assertEqual(type, "template")
        type = utils.get_stamp_type(avro, "difference")
        self.assertEqual(type, "difference")

    @mock.patch("stamp_service.fits2png.get_max")
    @mock.patch("stamp_service.fits2png.transform")
    def test_format_stamp(self, mock_transform, mock_get_max):
        mock_get_max.return_value = 1,2
        mock_transform.return_value = b"image"
        stamp = b"stamp"
        format = "fits"
        oid = "oid"
        candid = 123
        type = "type"
        stamp_file, mimetype, fname = utils.format_stamp(
            stamp, format, oid, candid, type
        )
        self.assertIsInstance(stamp_file, utils.io.BytesIO)
        self.assertEqual(mimetype, "application/fits+gzip")
        self.assertEqual(fname, "oid_123_type.fits.gz")
        format = "png"
        stamp_file, mimetype, fname = utils.format_stamp(
            stamp, format, oid, candid, type
        )
        self.assertIsInstance(stamp_file, utils.io.BytesIO)
        self.assertEqual(mimetype, "image/png")
        self.assertEqual(fname, "oid_123_type.png")
