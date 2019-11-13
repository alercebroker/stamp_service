import io
import os
import fastavro
from flask import Flask,request,send_file,Response,jsonify
from flask_cors import CORS
from . import fits2png
import requests
import wget

application = Flask(__name__)
CORS(application)

def _put_from_mars(oid,candid):
    application.logger.debug("Downloading {}/{}.avro".format(oid,candid))
    url_path = "https://mars.lco.global/?candid={}&format=json".format(candid)

    resp = requests.get(url_path)
    resp_json = resp.json()
    #application.logger.warning(resp_json)
    try:
        download_path = resp_json["results"][0]["avro"]
        mars_oid = resp_json["results"][0]["objectId"]
        mars_candid = resp_json["results"][0]["candid"]

        if mars_oid != oid:
            return False

        if mars_candid != candid:
            return False

        output_directory = oid2dir(oid)
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        file_name = '{}.avro'.format(candid)
        output_path = os.path.join(output_directory,file_name)
        application.logger.warning("Saving on {} from MARS oid={} candid={}".format(output_path,oid,candid))
        wget.download(download_path,output_path)
        application.logger.debug("Downloaded")
    except:
        return False
    return True

def oid2dir(oid):

    disks = 8
    disk = hash(oid) % disks
    output_directory = '{}/{}'.format(os.environ["AVRO_ROOT"],disk)
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

    if candid is None or oid is None or candid=="" or oid=="":
        return Response("{'status':'ERROR', 'content': 'Query Malformed'}",400)

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
        success = _put_from_mars(oid,candid)

        if not success:
            return Response("{'status':'ERROR', 'content': 'File not found'}",500)


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
        fname=f"{oid}_{candid}_{stamp_type}.fits.gz"
    if stamp_format == 'png':
        max_val,min_val = fits2png.get_max(data['cutoutScience']['stampData'],window)
        image_bytes = fits2png.transform(compressed_fits_bytes,stamp_type,max_val,min_val)
        stamp_file = io.BytesIO(image_bytes)
        mimetype = 'image/png'
        fname=f"{oid}_{candid}_{stamp_type}.png"


    return send_file(stamp_file,mimetype=mimetype,attachment_filename=fname,as_attachment=True)

@application.route('/put_avro',methods=['POST'])
def put_avro():

    args = request.args

    oid        = args.get('oid')
    candid     = args.get('candid')

    output_directory = oid2dir(oid)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory, exist_ok=True)


    file_name = '{}.avro'.format(candid)
    output_path = os.path.join(output_directory,file_name)

    if os.path.exists(output_path):
        return  "AVRO ALREADY EXISTS"

    application.logger.warning("Saving on {} from Stream oid={} candid={}".format(output_path,oid,candid))

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
    fname = f"{oid}_{candid}.avro"
    return send_file(avro_file,mimetype='application/avro+binary',attachment_filename=fname,as_attachment=True)



@application.route('/get_avro_info',methods=['GET'])
def get_avro_info():

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
    reader = fastavro.reader(avro_file)
    data = reader.next()
    del data["cutoutScience"]
    del data["cutoutTemplate"]
    del data["cutoutDifference"]

    return jsonify(data)



if __name__=="__main__":
    application.run(debug=True)
