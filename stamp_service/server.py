import io
import os
import fastavro
from flask import Flask, request, send_file, Response, jsonify
from flask_cors import CORS
import requests
import wget
from flask_restx import Api, Resource, reqparse
from werkzeug.exceptions import NotFound
from werkzeug.datastructures import FileStorage
from .search import S3Searcher, MARSSearcher, DiscSearcher
from . import utils

application = Flask(__name__)
CORS(application)
api = Api(
    application,
    version="1.0.0",
    title="ALeRCE AVRO Service",
    description="API for retreiving avro information and stamps.",
)

s3_searcher = S3Searcher(bucket_name=os.environ["BUCKET_NAME"])
mars_searcher = MARSSearcher(mars_url=os.environ["MARS_URL"])
disc_searcher = None
if os.getenv("USE_DISK", False):
    disc_searcher = DiscSearcher(
        root_path=os.environ["ROOT_PATH"], ndisk=os.environ["NDISK"]
    )

stamp_parser = reqparse.RequestParser()
stamp_parser.add_argument("oid", type=str, help="Object ID")
stamp_parser.add_argument("candid", type=int, help="Alert id")
stamp_parser.add_argument(
    "type", type=str, help="Stamp type", choices=["science", "template", "difference"]
)
stamp_parser.add_argument(
    "format", type=str, help="Stamp type", choices=["png", "fits"]
)

avro_parser = reqparse.RequestParser()
avro_parser.add_argument("oid", type=str, help="Object ID")
avro_parser.add_argument("candid", type=int, help="Alert id")

upload_parser = reqparse.RequestParser()
upload_parser.add_argument("candid", type=int, help="Alert id", location="form")
upload_parser.add_argument("avro", location="files", type=FileStorage, required=True)


@api.route("/get_stamp")
@api.response(200, "Success")
@api.response(404, "AVRO not found")
class StampResource(Resource):
    @api.expect(stamp_parser, validate=True)
    def get(self):
        args = stamp_parser.parse_args()
        # Search in s3
        try:
            data = s3_searcher.get_file_from_s3(args["oid"], args["candid"])
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
            application.logger.info(
                f"AVRO {args['candid']} not found in S3. Searching in disc."
            )
        # Search in disc
        if disc_searcher:
            try:
                input_directory = utils.oid2dir(
                    args["oid"], disc_searcher.root_path, disc_searcher.ndisk
                )
                file_name = "{}.avro".format(args["candid"])
                input_path = os.path.join(input_directory, file_name)
                data = disc_searcher.get_file_from_disc(input_path)
                data = utils.get_stamp_type(data, args["type"])
                print("DATA", data)
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
                application.logger.info(
                    f"AVRO {args['candid']} not found in disc. Searching in MARS"
                )
        # Search in MARS
        try:
            data = mars_searcher.get_file_from_mars(args["oid"], args["candid"])
            stamp_data = utils.get_stamp_type(data, args["type"])
            avro_file = io.BytesIO(data)
            reader = fastavro.reader(avro_file)
            data = reader.next()
        except Exception as e:
            application.logger.error("File could not be retreived from any source.")
            application.logger.error(f"Error: {e}")
            raise NotFound("AVRO not found")

        # Upload to S3 from MARS
        try:
            application.logger.info("Uploading Avro from MARS to S3")
            reverse_candid = utils.reverse_candid(args["candid"])
            file_name = "{}.avro".format(reverse_candid)
            s3_searcher.upload_file(avro_file, file_name)
            stamp_file, mimetype, fname = utils.format_stamp(
                stamp_data, args["format"], args["oid"], args["candid"], args["type"]
            )
            return send_file(
                stamp_file,
                mimetype=mimetype,
                attachment_filename=fname,
                as_attachment=True,
            )
            return jsonify(data)
        except Exception as e:
            raise e


@api.route("/get_avro_info")
@api.response(200, "Success")
@api.response(404, "AVRO not found")
class GetAVROInfoResource(Resource):
    @api.expect(avro_parser)
    def get(self):
        args = avro_parser.parse_args()
        try:
            data = s3_searcher.get_file_from_s3(args["oid"], args["candid"])
            avro_file = io.BytesIO(data)
            reader = fastavro.reader(avro_file)
            data = reader.next()
            del data["cutoutScience"]
            del data["cutoutTemplate"]
            del data["cutoutDifference"]
            return jsonify(data)
        except FileNotFoundError:
            application.logger.info(
                f"AVRO {args['candid']} not found in S3. Searching in disc."
            )

        if disc_searcher:
            try:
                input_directory = utils.oid2dir(
                    args["oid"], disc_searcher.root_path, disc_searcher.ndisk
                )
                file_name = "{}.avro".format(args["candid"])
                input_path = os.path.join(input_directory, file_name)
                data = disc_searcher.get_file_from_disc(input_path)
                avro_file = io.BytesIO(data)
                reader = fastavro.reader(avro_file)
                data = reader.next()
                del data["cutoutScience"]
                del data["cutoutTemplate"]
                del data["cutoutDifference"]
                return jsonify(data)
            except FileNotFoundError:
                application.logger.info(
                    f"AVRO {args['candid']} not found in disc. Searching in MARS"
                )
        try:
            data = mars_searcher.get_file_from_mars(args["oid"], args["candid"])
            avro_file = io.BytesIO(data)
            reader = fastavro.reader(avro_file)
            data = reader.next()
            del data["cutoutScience"]
            del data["cutoutTemplate"]
            del data["cutoutDifference"]
        except Exception as e:
            application.logger.error("File could not be retreived from any source.")
            application.logger.error(f"Error: {e}")
            raise NotFound("AVRO not found")
        try:
            application.logger.info("Uploading Avro from MARS to S3")
            reverse_candid = utils.reverse_candid(args["candid"])
            file_name = "{}.avro".format(reverse_candid)
            s3_searcher.upload_file(avro_file, file_name)
            return jsonify(data)
        except Exception as e:
            raise e


@api.route("/put_avro")
@api.response(200, "Success")
@api.response(500, "Server error")
class PutAVROResource(Resource):
    @api.expect(upload_parser)
    def post(self):
        args = upload_parser.parse_args()
        reverse_candid = utils.reverse_candid(args["candid"])
        file_name = "{}.avro".format(reverse_candid)
        application.logger.info(
            "Saving on s3://{}/{}".format(s3_searcher.bucket_name, file_name)
        )
        f = args["avro"]
        try:
            response = s3_searcher.upload_file(f, object_name=file_name)
            return jsonify(response)
        except Exception as e:
            raise e


@api.route("/get_avro")
@api.response(200, "Success")
@api.response(404, "AVRO not found")
class GetAVROResource(Resource):
    @api.expect(avro_parser)
    def get(self):
        args = avro_parser.parse_args()
        try:
            data = s3_searcher.get_file_from_s3(args["oid"], args["candid"])
            avro_file = io.BytesIO(data)
            reverse_candid = utils.reverse_candid(args["candid"])
            fname = f"{reverse_candid}.avro"
            return send_file(
                avro_file,
                mimetype="application/avro+binary",
                attachment_filename=fname,
                as_attachment=True,
            )
        except FileNotFoundError:
            application.logger.info(
                f"AVRO {args['candid']} not found in S3. Searching in disc."
            )

        if disc_searcher:
            try:
                input_directory = utils.oid2dir(
                    args["oid"], disc_searcher.root_path, disc_searcher.ndisk
                )
                file_name = "{}.avro".format(args["candid"])
                input_path = os.path.join(input_directory, file_name)
                data = disc_searcher.get_file_from_disc(input_path)
                avro_file = io.BytesIO(data)
                reverse_candid = utils.reverse_candid(args["candid"])
                fname = f"{reverse_candid}.avro"
                return send_file(
                    avro_file,
                    mimetype="application/avro+binary",
                    attachment_filename=fname,
                    as_attachment=True,
                )
            except FileNotFoundError:
                application.logger.info(
                    f"AVRO {args['candid']} not found in disc. Searching in MARS"
                )
        try:
            data = mars_searcher.get_file_from_mars(args["oid"], args["candid"])
            avro_file = io.BytesIO(data)
        except Exception as e:
            application.logger.error("File could not be retreived from any source.")
            application.logger.error(f"Error: {e}")
            raise NotFound("AVRO not found")
        try:
            application.logger.info("Uploading Avro from MARS to S3")
            reverse_candid = utils.reverse_candid(args["candid"])
            file_name = "{}.avro".format(reverse_candid)
            s3_searcher.upload_file(avro_file, file_name)
            return send_file(
                avro_file,
                mimetype="application/avro+binary",
                attachment_filename=file_name,
                as_attachment=True,
            )
        except Exception as e:
            raise e
