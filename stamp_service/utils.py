import io
from . import fits2png
from envyaml import EnvYAML


def format_stamp(stamp, fmt, oid, candid, stype):
    window = 2
    basename = f'{oid}_{candid}_{stype}'
    if fmt == "fits":
        stamp_file = io.BytesIO(stamp)
        mimetype = "application/fits+gzip"
        fname = f"{basename}.fits.gz"
    elif fmt == "png":
        stamp_file = io.BytesIO(fits2png.transform(stamp, stype, window))
        mimetype = "image/png"
        fname = f"{basename}.png"
    else:
        raise ValueError(f'Unrecognized format {fmt}')
    return stamp_file, mimetype, fname


def get_stamp_type(avro, stype):
    if stype == 'science':
        key = 'cutoutScience'
    elif stype == 'template':
        key = 'cutoutTemplate'
    elif stype == 'difference':
        key = 'cutoutDifference'
    else:
        raise ValueError(f'Unrecognized stamp type {stype}')
    return avro[key]['stampData']


def reverse_candid(candid):
    """
    Returns reverse digits of the candid

    Parameters
    ----------
    candid : int or str
        original candid to be reversed
    """
    return str(candid)[::-1]

def get_configuration_object(config_file_path):

    yaml_config = EnvYAML(config_file_path)
    try:
        import boto3
        client = boto3.client("s3")
        # try to donwload the config file yaml_config["remote_file_"]
        client.download_file(
            yaml_config['remote_file_bucket'],
            yaml_config['remote_file_name'],
            'remote_config_file.yml'
        )
        # pendiente probar si se puede usar el diccionario en memoria
        remote_yaml_config = EnvYAML('remote_config_file')
        return remote_yaml_config['SERVER_SETTINGS']
    except Exception as e:
        # if cant be downloaded and loaded use the rest of the local file
        print(f"error {repr(e)}") # use logger
        return yaml_config['SERVER_SETTINGS']
