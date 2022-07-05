from ast import arg
import copy
from flask_restx import Resource, reqparse, Api
from werkzeug.exceptions import NotFound
from werkzeug.datastructures import FileStorage
from . import utils
from .search import s3_searcher, mars_searcher
from flask import current_app as app
from flask import send_file, jsonify

import fastavro

stamp_parser = reqparse.RequestParser()
stamp_parser.add_argument("oid", type=str, help="Object ID", required=True)
stamp_parser.add_argument("candid", type=str, help="Alert id", required=True)
stamp_parser.add_argument(
    "type",
    type=str,
    help="Stamp type",
    choices=["science", "template", "difference"],
    required=True,
)
stamp_parser.add_argument(
    "format",
    type=str,
    help="Stamp type",
    choices=["png", "fits"],
    required=True,
)
stamp_parser.add_argument(
    "survey_id",
    type=str,
    help="Survey ID",
    choices=["ztf", "atlas"],
    default="ztf",
)

avro_parser = reqparse.RequestParser()
avro_parser.add_argument("oid", type=str, help="Object ID", required=True)
avro_parser.add_argument("candid", type=str, help="Alert id", required=True)
avro_parser.add_argument(
    "survey_id",
    type=str,
    help="Survey ID",
    choices=["ztf", "atlas"],
    default="ztf",
)

upload_parser = reqparse.RequestParser()
upload_parser.add_argument(
    "candid", type=str, help="Alert id", location="form", required=True
)
upload_parser.add_argument("avro", location="files", type=FileStorage, required=True)
upload_parser.add_argument(
    "survey_id",
    type=str,
    help="Survey ID",
    choices=["ztf", "atlas"],
    required=True,
)

api = Api(
    version="1.0.0",
    title="ALeRCE AVRO Service",
    description="API for retrieving avro information and stamps.",
)


@api.route("/get_stamp")
@api.response(200, "Success")
@api.response(404, "AVRO not found")
class StampResource(Resource):
    @api.expect(stamp_parser, validate=True)
    def get(self):
        args = stamp_parser.parse_args()
        # Search in s3
        try:
            data = s3_searcher.get_file_from_s3(args["candid"], args['survey_id'])
            data = next(fastavro.reader(data))
            data = utils.get_stamp_type(data, args["type"])
            stamp_file, mimetype, fname = utils.format_stamp(
                data, args["format"], args["oid"], args["candid"], args["type"]
            )
            app.logger.info(f"[HIT] AVRO {args['candid']} found in S3.")
            return send_file(
                stamp_file,
                mimetype=mimetype,
                download_name=fname,
                as_attachment=True,
            )
        except FileNotFoundError:
            app.logger.info(f"[MISS] AVRO {args['candid']} not found in S3.")
        # Search in MARS
        try:
            avro_io = mars_searcher.get_file_from_mars(args["oid"], args["candid"])
            data = next(fastavro.reader(avro_io))
            stamp_data = utils.get_stamp_type(data, args["type"])
        except Exception as e:
            app.logger.info(
                f"[MISS] AVRO {args['candid']} could not be retrieved from MARS."
            )
            raise NotFound("AVRO not found")

        # Upload to S3 from MARS
        try:
            app.logger.info(
                f"[HIT] AVRO {args['candid']} found in MARS. Uploading from MARS to S3"
            )
            reverse_candid = utils.reverse_candid(args["candid"])
            file_name = "{}.avro".format(reverse_candid)
            s3_searcher.upload_file(avro_io, file_name, args['survey_id'])
            stamp_file, mimetype, fname = utils.format_stamp(
                stamp_data, args["format"], args["oid"], args["candid"], args["type"]
            )
            return send_file(
                stamp_file,
                mimetype=mimetype,
                download_name=fname,
                as_attachment=True,
            )
        except Exception as e:
            app.logger.info("Could not upload file to S3")
            raise e


@api.route("/get_avro_info")
@api.response(200, "Success")
@api.response(404, "AVRO not found")
class GetAVROInfoResource(Resource):
    @api.expect(avro_parser)
    def get(self):
        args = avro_parser.parse_args()
        try:
            data = s3_searcher.get_file_from_s3(args["candid"], args['survey_id'])
            data = next(fastavro.reader(data))
            if args['survey_id'] == "ztf":
                del data["cutoutTemplate"] # este atlas no lo tiene
            del data["cutoutScience"]
            del data["cutoutDifference"]
            app.logger.info(f"[HIT] AVRO {args['candid']} found in S3.")
            data["candidate"]["candid"] = str(data["candidate"]["candid"])
            return jsonify(data)
        except FileNotFoundError:
            app.logger.info(f"[MISS] AVRO {args['candid']} not found in S3.")

        try:
            avro_io = mars_searcher.get_file_from_mars(args["oid"], args["candid"])
            data = next(fastavro.reader(avro_io))
            del data["cutoutScience"]
            del data["cutoutTemplate"]
            del data["cutoutDifference"]
        except Exception as e:
            app.logger.info(
                f"[MISS] AVRO {args['candid']} could not be retrieved from MARS."
            )
            app.logger.error(f"Error: {e}")
            raise NotFound("AVRO not found")
        try:
            app.logger.info("Uploading Avro from MARS to S3")
            reverse_candid = utils.reverse_candid(args["candid"])
            file_name = "{}.avro".format(reverse_candid)
            s3_searcher.upload_file(avro_io, file_name, args['survey_id'])
            data["candidate"]["candid"] = str(data["candidate"]["candid"])
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
        app.logger.info(
            "Saving on s3://{}/{}".format(s3_searcher.buckets_dict[args['survey_id']]['bucket'], file_name)
        )
        f = args["avro"]
        try:
            response = s3_searcher.upload_file(f, object_name=file_name, survey_id=args['survey_id'])
            return jsonify(response)
        except Exception as e:
            app.logger.info("Could not upload file to S3")
            app.logger.error(e)
            raise e


@api.route("/get_avro")
@api.response(200, "Success")
@api.response(404, "AVRO not found")
class GetAVROResource(Resource):
    @api.expect(avro_parser)
    def get(self):
        args = avro_parser.parse_args()
        try:
            data = s3_searcher.get_file_from_s3(args["candid"], args['survey_id'])
            fname = f"{args['candid']}.avro"
            app.logger.info(f"[HIT] AVRO {args['candid']} found in S3")
            return send_file(
                data,
                mimetype="app/avro+binary",
                download_name=fname,
                as_attachment=True,
            )
        except FileNotFoundError:
            app.logger.info(f"AVRO {args['candid']} not found in S3.")

        try:
            avro_io = mars_searcher.get_file_from_mars(args["oid"], args["candid"])
            app.logger.info(f"[HIT] AVRO {args['candid']} found in MARS")
        except Exception as e:
            app.logger.info("File could not be retreived from MARS.")
            app.logger.error(f"Error: {e}")
            raise NotFound("AVRO not found")
        try:
            app.logger.info("Uploading Avro from MARS to S3")
            reverse_candid = utils.reverse_candid(args["candid"])
            file_name = "{}.avro".format(reverse_candid)
            avro_io2 = copy.deepcopy(avro_io)  # Next step closes buffer
            s3_searcher.upload_file(avro_io, file_name, args['survey_id'])
            file_name = f"{args['candid']}.avro"
            return send_file(
                avro_io2,
                mimetype="app/avro+binary",
                download_name=file_name,
                as_attachment=True,
            )
        except Exception as e:
            raise e
