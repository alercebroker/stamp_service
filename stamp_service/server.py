import io
import os
import fastavro
from flask import Flask, request, send_file, Response, jsonify
from flask_cors import CORS
import requests
import wget
import boto3
import boto3.s3
from botocore.exceptions import ClientError
from flask_restx import Api, Resource, reqparse
from werkzeug.exceptions import NotFound
from .search import S3Searcher, MARSSearcher, DiscSearcher
from . import utils

bucket_name = os.environ["BUCKET_NAME"]

application = Flask(__name__)
CORS(application)
api = Api(
    application,
    version="1.0.0",
    title="ALeRCE AVRO Service",
    description="API for retreiving avro information and stamps.",
)

s3_searcher = S3Searcher()
mars_searcher = MARSSearcher()
disc_searcher = DiscSearcher()

parser = reqparse.RequestParser()
parser.add_argument("oid", type=str, help="Object ID")
parser.add_argument("candid", type=int, help="Alert id")
parser.add_argument(
    "type", type=str, help="Stamp type", choices=["science", "template", "difference"]
)
parser.add_argument("format", type=str, help="Stamp type", choices=["png", "fits"])


def _put_from_mars(oid, candid):
    application.logger.debug("Downloading {}/{}.avro".format(oid, candid))
    url_path = "https://mars.lco.global/?candid={}&format=json".format(candid)

    try:
        output_directory = oid2dir(oid)
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        file_name = "{}.avro".format(candid)
        output_path = os.path.join(output_directory, file_name)
        application.logger.warning(
            "Saving on {} from MARS oid={} candid={}".format(output_path, oid, candid)
        )
        wget.download(download_path, output_path)
        application.logger.debug("Downloaded")
    except:
        return False
    return True


@api.route("/get_stamp")
@api.response(200, "Success")
@api.response(404, "AVRO not found")
class StampResource(Resource):
    @api.expect(parser, validate=True)
    def get(self):
        args = parser.parse_args()
        try:
            data = s3_searcher.get_avro_from_s3(args["oid"], args["candid"])
            data = utils.get_stamp_type(data, args["type"])
            stamp_file, mimetype, fname = utils.format_stamp(
                data, args["format"], args["oid"], args["candid"], args["type"]
            )
            return send_file(
                stamp_file,
                mimetype=mimetype,
                attachment_filename=fname,
                as_attachment=True,
            )
        except FileNotFoundError:
            self.logger.info(
                f"AVRO {args['candid']} not found in S3. Searching in disc."
            )
        try:
            input_directory = utils.oid2dir(args["oid"])
            file_name = "{}.avro".format(args["candid"])
            input_path = os.path.join(input_directory, file_name)
            data = disc_searcher.get_file_from_disc(input_path)
            data = utils.get_stamp_type(data, args["type"])
            stamp_file, mimetype, fname = utils.format_stamp(
                data, args["format"], args["oid"], args["candid"], args["type"]
            )
            return send_file(
                stamp_file,
                mimetype=mimetype,
                attachment_filename=fname,
                as_attachment=True,
            )
        except FileNotFoundError:
            self.logger.info(
                f"AVRO {args['candid']} not found in disc. Searching in MARS"
            )

        try:
            data = mars_searcher.get_file_from_mars(args["oid"], args["candid"])
            data = utils.get_stamp_type(data, args["type"])
            stamp_file, mimetype, fname = utils.format_stamp(
                data, args["format"], args["oid"], args["candid"], args["type"]
            )
            return send_file(
                stamp_file,
                mimetype=mimetype,
                attachment_filename=fname,
                as_attachment=True,
            )
        except Exception as e:
            self.logger.error("File could not be retreived from any source.")
            self.logger.error(f"Error: {e}")
            raise NotFound("AVRO not found")


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        application.logger.error(e)
        return False
    return True



class AVROResource(Resource):
    @application.route("/get_avro_from_s3", methods=["GET"])
    def get_avro_from_s3():

        file_name = "{}.avro".format(candid)
        #    input_path = os.path.join(input_directory,file_name)
        object_name = oid + "/" + file_name

        #    data = None
        avro_file = io.BytesIO()
        s3_client = boto3.client("s3")
        try:
            s3_client.download_fileobj(bucket_name, object_name, avro_file)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                # The object does not exist.
                raise FileNotFoundError
            else:
                # Something else has gone wrong.
                raise

        #   try:
        #       with open(input_path,'rb') as f:
        #           data = f.read()
        #   except FileNotFoundError:
        #       _put_from_mars(oid,candid)
        #       with open(input_path,'rb') as f:
        #           data = f.read()

        #    avro_file = io.BytesIO(data)
        fname = f"{oid}_{candid}.avro"
        return send_file(
            avro_file,
            mimetype="application/avro+binary",
            attachment_filename=fname,
            as_attachment=True,
        )

    @application.route("/get_avro_info", methods=["GET"])
    def get_avro_info():

        args = request.args

        oid = args.get("oid")
        candid = int(args.get("candid"))

        input_directory = oid2dir(oid)

        file_name = "{}.avro".format(candid)
        input_path = os.path.join(input_directory, file_name)

        data = None
        try:
            with open(input_path, "rb") as f:
                data = f.read()
        except FileNotFoundError:
            _put_from_mars(oid, candid)
            with open(input_path, "rb") as f:
                data = f.read()

        avro_file = io.BytesIO(data)
        reader = fastavro.reader(avro_file)
        data = reader.next()
        del data["cutoutScience"]
        del data["cutoutTemplate"]
        del data["cutoutDifference"]

        return jsonify(data)

    @application.route("/put_avro", methods=["POST"])
    def put_avro():

        args = request.args

        oid = args.get("oid")
        candid = int(args.get("candid"))

        #    output_directory = oid2dir(oid)
        #    if not os.path.exists(output_directory):
        #        os.makedirs(output_directory, exist_ok=True)

        file_name = "{}.avro".format(candid)
        #    output_path = os.path.join(output_directory,file_name)
        object_name = oid + "/" + file_name

        #    if os.path.exists(output_path):
        #        return  "AVRO ALREADY EXISTS"

        #    application.logger.warning("Saving on {} from Stream oid={} candid={}".format(output_path,oid,candid))
        application.logger.warning("Saving on s3://{}/{}".format(bucket_name, object_name))

        f = request.files["avro"]

        #    f.save(output_path)

        upload_file(f, bucket_name, object_name)

        return "AVRO SAVED"


