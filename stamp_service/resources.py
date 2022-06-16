from flask_restx import Resource, reqparse, Api
from werkzeug.exceptions import NotFound
from werkzeug.datastructures import FileStorage
from . import utils
from .search import s3_searcher, mars_searcher, disc_searcher
from flask import current_app as app
from flask import send_file, jsonify
from urllib.request import urlopen

import fastavro
import os

stamp_parser = reqparse.RequestParser()
stamp_parser.add_argument("oid", type=str, help="Object ID", required=True)
stamp_parser.add_argument("candid", type=int, help="Alert id", required=True)
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
    "surveyid",
    type=str,
    help="Survey ID",
    choices=["ztf", "atlas"],
    default="ztf",
)

avro_parser = reqparse.RequestParser()
avro_parser.add_argument("oid", type=str, help="Object ID", required=True)
avro_parser.add_argument("candid", type=int, help="Alert id", required=True)
avro_parser.add_argument(
    "surveyid",
    type=str,
    help="Survey ID",
    choices=["ztf", "atlas"],
    default="ztf",
)

upload_parser = reqparse.RequestParser()
upload_parser.add_argument(
    "candid", type=int, help="Alert id", location="form", required=True
)
upload_parser.add_argument("avro", location="files", type=FileStorage, required=True)
upload_parser.add_argument(
    "surveyid",
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
            data = s3_searcher.get_file_from_s3(args["candid"])
            data = fastavro.reader(data).next()
            data = utils.get_stamp_type(data, args["type"])
            stamp_file, mimetype, fname = utils.format_stamp(
                data, args["format"], args["oid"], args["candid"], args["type"]
            )
            app.logger.info(f"[HIT] AVRO {args['candid']} found in S3.")
            return send_file(
                stamp_file,
                mimetype=mimetype,
                attachment_filename=fname,
                as_attachment=True,
            )
        except FileNotFoundError:
            app.logger.info(f"[MISS] AVRO {args['candid']} not found in S3.")
        # Search in disc
        if os.getenv("USE_DISK", False):
            try:
                input_directory = utils.oid2dir(
                    args["oid"], disc_searcher.root_path, disc_searcher.ndisk
                )
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
                app.logger.info(
                    f"[MISS] AVRO {args['candid']} not found in disc. Searching in MARS"
                )
        # Search in MARS
        try:
            avro_file = mars_searcher.get_file_from_mars(args["oid"], args["candid"])
            avro_io = mars_searcher.opener.open(avro_file)
            data = fastavro.reader(avro_io).next()
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
            s3_searcher.upload_file(urlopen(avro_file), file_name)
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
            data = s3_searcher.get_file_from_s3(args["candid"])
            data = fastavro.reader(data).next()
            del data["cutoutScience"]
            del data["cutoutTemplate"]
            del data["cutoutDifference"]
            app.logger.info(f"[HIT] AVRO {args['candid']} found in S3.")
            data["candidate"]["candid"] = str(data["candidate"]["candid"])
            return jsonify(data)
        except FileNotFoundError:
            app.logger.info(f"[MISS] AVRO {args['candid']} not found in S3.")

        if os.getenv("USE_DISK", False):
            try:
                input_directory = utils.oid2dir(
                    args["oid"], disc_searcher.root_path, disc_searcher.ndisk
                )
                file_name = "{}.avro".format(args["candid"])
                input_path = os.path.join(input_directory, file_name)
                data = disc_searcher.get_file_from_disc(input_path)
                del data["cutoutScience"]
                del data["cutoutTemplate"]
                del data["cutoutDifference"]
                app.logger.info(f"[HIT] AVRO {args['candid']} found in disk.")
                data["candidate"]["candid"] = str(data["candidate"]["candid"])
                return jsonify(data)
            except FileNotFoundError:
                app.logger.info(
                    f"[MISS] AVRO {args['candid']} not found in disk. Searching in MARS"
                )
        try:
            avro_file = mars_searcher.get_file_from_mars(args["oid"], args["candid"])
            avro_io = mars_searcher.opener.open(avro_file)
            data = fastavro.reader(avro_io).next()
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
            s3_searcher.upload_file(avro_io, file_name)
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
            "Saving on s3://{}/{}".format(s3_searcher.bucket_name, file_name)
        )
        f = args["avro"]
        try:
            response = s3_searcher.upload_file(f, object_name=file_name)
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
            data = s3_searcher.get_file_from_s3(args["candid"])
            fname = f"{args['candid']}.avro"
            app.logger.info(f"[HIT] AVRO {args['candid']} found in S3")
            return send_file(
                data,
                mimetype="app/avro+binary",
                attachment_filename=fname,
                as_attachment=True,
            )
        except FileNotFoundError:
            app.logger.info(f"AVRO {args['candid']} not found in S3.")

        if os.getenv("USE_DISK", False):
            try:
                input_directory = utils.oid2dir(
                    args["oid"], disc_searcher.root_path, disc_searcher.ndisk
                )
                file_name = "{}.avro".format(args["candid"])
                input_path = os.path.join(input_directory, file_name)
                data = disc_searcher.get_raw_file_from_disc(input_path)
                fname = f"{args['candid']}.avro"
                app.logger.info(f"[HIT] AVRO {args['candid']} found in disk.")
                return send_file(
                    data,
                    mimetype="app/avro+binary",
                    attachment_filename=fname,
                    as_attachment=True,
                )
            except FileNotFoundError:
                app.logger.info(
                    f"AVRO {args['candid']} not found in disc. Searching in MARS"
                )
        try:
            avro_file = mars_searcher.get_file_from_mars(args["oid"], args["candid"])
            avro_io = mars_searcher.opener.open(avro_file)
            app.logger.info(f"[HIT] AVRO {args['candid']} found in MARS")
        except Exception as e:
            app.logger.info("File could not be retreived from MARS.")
            app.logger.error(f"Error: {e}")
            raise NotFound("AVRO not found")
        try:
            app.logger.info("Uploading Avro from MARS to S3")
            reverse_candid = utils.reverse_candid(args["candid"])
            file_name = "{}.avro".format(reverse_candid)
            s3_searcher.upload_file(avro_io, file_name)
            file_name = f"{args['candid']}.avro"
            return send_file(
                avro_io,
                mimetype="app/avro+binary",
                attachment_filename=file_name,
                as_attachment=True,
            )
        except Exception as e:
            raise e
