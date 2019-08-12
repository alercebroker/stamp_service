import io
import os
import fastavro
from flask import Flask,request,send_file,Response
from flask_cors import CORS
import fits2png_simple as fits2png
import requests
import wget


application = Flask(__name__)
CORS(application)

def _put_from_mars(oid,candid):
    application.logger.debug("Downloading {}/{}.avro".format(oid,candid))
    url_path = "https://mars.lco.global/?candid={}&format=json".format(candid)

    resp = requests.get(url_path)
    resp_json = resp.json()
    download_path = resp_json["results"][0]["avro"]

    output_directory = oid2dir(oid)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    file_name = '{}.avro'.format(candid)
    output_path = os.path.join(output_directory,file_name)
    wget.download(download_path,output_path)
    application.logger.debug("Downloaded")

    return

def oid2dir(oid):

    disks = 8
    disk = hash(oid) % disks
    output_directory = '/mnt/stamps/{}'.format(disk)
    output_path = oid[:5]
    for c in oid[5:]:
        output_path = os.path.join(output_path, c)

    output_path = os.path.join( output_directory, output_path )

    return output_path

@application.route('/')
def index():
    return "STAMPS API"

@application.route('/get_stamp',methods=['GET'])
def get_stamp():

    #arguments
    args =  request.args
    if "oid" not in args or "candid" not in args or "type" not in args or "format" not in args:
        return Response("{'status':'ERROR', 'content': 'Query Malformed'}",400)

    oid = args.get('oid')
    candid       = args.get('candid')
    stamp_type   = args.get('type')
    stamp_format = args.get('format')

    if stamp_format not in ["fits","png"]:
        return Response("{'status':'ERROR', 'content': 'Format not supported, only png or compressed fits (.tar.gz)'}",400)

    input_directory = oid2dir(oid)

    file_name = '{}.avro'.format(candid)
    input_path = os.path.join(input_directory,file_name)

    data = None
    try:
        with open(input_path,'rb') as f:
            data = fastavro.reader(f).next()
    except FileNotFoundError:
        _put_from_mars(oid,candid)
        with open(input_path,'rb') as f:
            data = fastavro.reader(f).next()
        # return Response("{'status':'ERROR', 'content': 'File not found'}",400)
    #type
    stamp = None
    if stamp_type == 'science':
        stamp = data['cutoutScience']['stampData']
    elif stamp_type == 'template':
        stamp = data['cutoutTemplate']['stampData']
    elif stamp_type == 'difference':
        stamp = data['cutoutDifference']['stampData']

    window = 2
    #format
    compressed_fits_bytes = stamp
    stamp_file = None
    if stamp_format == 'fits':
        stamp_file = io.BytesIO(compressed_fits_bytes)
        mimetype = 'application/fits+gzip'
    if stamp_format == 'png':
        max_val,min_val = fits2png.get_max(data['cutoutScience']['stampData'],window)
        image_bytes = fits2png.transform(compressed_fits_bytes,stamp_type,max_val,min_val)
        stamp_file = io.BytesIO(image_bytes)
        mimetype = 'image/png'

    return send_file(stamp_file,mimetype=mimetype)

@application.route('/put_avro',methods=['POST'])
def put_avro():

    args = request.args

    oid        = args.get('oid')
    candid     = args.get('candid')

    output_directory = oid2dir(oid)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    file_name = '{}.avro'.format(candid)
    output_path = os.path.join(output_directory,file_name)

    print(output_path)

    f = request.files['avro']

    f.save(output_path)

    return "AVRO SAVED"

@application.route('/get_avro',methods=['GET'])
def get_avro():

    args =  request.args

    oid        = args.get('oid')
    candid     = args.get('candid')

    input_directory = oid2dir(oid)

    file_name = '{}.avro'.format(candid)
    input_path = os.path.join(input_directory,file_name)

    data = None
    try:
        with open(input_path,'rb') as f:
            data = f.read()
    except FileNotFoundError:
        _put_from_mars(oid,candid)
        with open(input_path,'rb') as f:
            data = f.read()

    avro_file = io.BytesIO(data)

    return send_file(avro_file,mimetype='application/avro+binary')

if __name__=="__main__":
    application.run(debug=True)
