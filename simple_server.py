import os
import bson
import fastavro
from quart import Quart,request

def oid2dir(oid):

    disks = 8
    disk = hash(oid) % disks
    output_directory = '/mnt/stamps/{}'.format(disk)
    output_path = oid[:5]
    for c in oid[5:]:
        output_path = os.path.join(output_path, c)

    output_path = os.path.join( output_directory, output_path )

    return output_path

app = Quart(__name__)

@app.route('/')
async def index():
    return "STAMPS API"

@app.route('/get_stamps',methods=['POST'])
async def get_stamps():

    data = await request.data
    param = bson.loads(data)

    oid    = param['oid']
    candid = param['candid']

    input_directory = oid2dir(oid)
    file_name = '{}.avro'.format(candid)
    input_path = os.path.join(input_directory,file_name)

    data = None
    with open(input_path,'rb') as f:
        data = fastavro.reader(f).next()

    stamps = {
            'cutoutScience' : data['cutoutScience'], 
            'cutoutTemplate' : data['cutoutTemplate'], 
            'cutoutDifference' : data['cutoutDifference']
    }

    return bson.dumps(stamps)

@app.route('/get_avro',methods=['POST'])
async def get_avro():

    return "AVRO SAVED"

@app.route('/put_avro',methods=['POST'])
async def put_avro():

    data = await request.data
    param = bson.loads(data)

    oid    = param['oid']
    candid = param['candid']
    avro   = param['avro']

    output_directory = oid2dir(oid)
    file_name = '{}.avro'.format(candid)
    output_path = os.path.join(input_directory,file_name)

    with open(output_path,'wb') as f:
        f.write(avro)

    return "AVRO SAVED"
