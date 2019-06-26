import io
import os
import fastavro
from flask import Flask,request,send_file

def oid2dir(oid):

    disks = 8
    disk = hash(oid) % disks
    output_directory = '/mnt/stamps/{}'.format(disk)
    output_path = oid[:5]
    for c in oid[5:]:
        output_path = os.path.join(output_path, c)

    output_path = os.path.join( output_directory, output_path )

    return output_path

app = Flask(__name__)

@app.route('/')
def index():
    return "STAMPS API"

@app.route('/get_stamp',methods=['POST'])
def get_stamp():

    args =  request.args

    oid        = args.get('oid')
    candid     = args.get('candid')
    stamp_type = args.get('type')

    input_directory = oid2dir(oid)

    file_name = '{}.avro'.format(candid)
    input_path = os.path.join(input_directory,file_name)

    data = None
    with open(input_path,'rb') as f:
        data = fastavro.reader(f).next()

    stamp = None
    if stamp_type == 'science':
        stamp = data['cutoutScience']['stampData']
    elif stamp_type == 'template':
        stamp = data['cutoutTemplate']['stampData']
    elif stamp_type == 'difference':
        stamp = data['cutoutDifference']['stampData']

    stamp_file = io.BytesIO(stamp)

    return send_file(stamp_file,mimetype='application/fits+gzip')

@app.route('/put_avro',methods=['POST'])
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

@app.route('/get_avro',methods=['POST'])
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
