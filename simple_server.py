import io
import os
import fastavro
from flask import Flask,request,send_file
import flask_cors import CORS
import fits2png 

def oid2dir(oid):

    disks = 8
    disk = hash(oid) % disks
    output_directory = '/mnt/stamps/{}'.format(disk)
    output_path = oid[:5]
    for c in oid[5:]:
        output_path = os.path.join(output_path, c)

    output_path = os.path.join( output_directory, output_path )

    return output_path

application = Flask(__name__)
CORS(application)

@application.route('/')
def index():
    return "STAMPS API"

@application.route('/get_stamp',methods=['POST'])
def get_stamp():

    #arguments
    args =  request.args
    oid          = args.get('oid')
    candid       = args.get('candid')
    stamp_type   = args.get('type')
    stamp_format = args.get('format')

    input_directory = oid2dir(oid)

    file_name = '{}.avro'.format(candid)
    input_path = os.path.join(input_directory,file_name)

    data = None
    with open(input_path,'rb') as f:
        data = fastavro.reader(f).next()

    #type
    stamp = None
    if stamp_type == 'science':
        stamp = data['cutoutScience']['stampData']
    elif stamp_type == 'template':
        stamp = data['cutoutTemplate']['stampData']
    elif stamp_type == 'difference':
        stamp = data['cutoutDifference']['stampData']

    #format
    stamp_file = io.BytesIO(stamp)
    mimetype = 'application/fits+gzip'
    if stamp_format == 'png':
	stamp_file = fits2png.transform(stamp_file)
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

@application.route('/get_avro',methods=['POST'])
def get_avro():

    args =  request.args

    oid        = args.get('oid')
    candid     = args.get('candid')

    input_directory = oid2dir(oid)

    file_name = '{}.avro'.format(candid)
    input_path = os.path.join(input_directory,file_name)

    data = None
    with open(input_path,'rb') as f:
        data = f.read()

    avro_file = io.BytesIO(data)

    return send_file(avro_file,mimetype='application/avro+binary')
