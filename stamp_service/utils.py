import io
from . import fits2png


def format_stamp(stamp, fmt, oid, candid, stype):
    window = 2
    if oid:
        basename = f"{oid}_{candid}_{stype}"
    else:
        basename = f"{candid}_{stype}"
    if fmt == "fits":
        stamp_file = io.BytesIO(stamp)
        mimetype = "application/fits+gzip"
        fname = f"{basename}.fits.gz"
    elif fmt == "png":
        stamp_file = io.BytesIO(fits2png.transform(stamp, stype, window))
        mimetype = "image/png"
        fname = f"{basename}.png"
    else:
        raise ValueError(f"Unrecognized format {fmt}")
    return stamp_file, mimetype, fname


def get_stamp_type(avro, stype):
    if stype == "science":
        key = "cutoutScience"
    elif stype == "template":
        key = "cutoutTemplate"
    elif stype == "difference":
        key = "cutoutDifference"
    else:
        raise ValueError(f"Unrecognized stamp type {stype}")
    return avro[key]["stampData"]


def reverse_candid(candid):
    """
    Returns reverse digits of the candid

    Parameters
    ----------
    candid : int or str
        original candid to be reversed
    """
    return str(candid)[::-1]
