import io
from . import fits2png
import os


def format_stamp(stamp, format, oid, candid, type):
    window = 2
    stamp_file = None
    if format == "fits":
        stamp_file = io.BytesIO(stamp)
        mimetype = "application/fits+gzip"
        fname = f"{oid}_{candid}_{type}.fits.gz"
    if format == "png":
        max_val, min_val = fits2png.get_max(stamp, window)
        image_bytes = fits2png.transform(stamp, type, max_val, min_val)
        stamp_file = io.BytesIO(image_bytes)
        mimetype = "image/png"
        fname = f"{oid}_{candid}_{type}.png"
    return stamp_file, mimetype, fname


def get_stamp_type(avro, type):
    if type == "science":
        return avro["cutoutScience"]["stampData"]
    elif type == "template":
        return avro["cutoutTemplate"]["stampData"]
    elif type == "difference":
        return avro["cutoutDifference"]["stampData"]


def oid2dir(oid, avro_root, ndisk):
    disk = hash(oid) % ndisk
    output_directory = "{}/{}".format(avro_root, disk)
    if os.getenv("TEST_MODE", False):
        output_directory = avro_root
    output_path = oid[:5]
    for c in oid[5:]:
        output_path = os.path.join(output_path, c)
    output_path = os.path.join(output_directory, output_path)
    return output_path

def reverse_candid(candid):
    """
    Returns reverse digits of the candid

    Parameters
    ----------
    candid : int
        original candid to be reversed
    """
    n = int(candid)
    reversed = int(str(n)[::-1])
    return reversed

