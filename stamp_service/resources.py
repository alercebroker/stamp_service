import io

from flask_restx import Resource, reqparse, Api
from werkzeug.exceptions import NotFound
from werkzeug.datastructures import FileStorage
from . import utils
from .search import s3_searcher, mars_searcher
from flask import current_app as app
from flask import send_file, jsonify
from ralidator_flask.decorators import (
    set_permissions_decorator,
    check_permissions_decorator,
)
from .filters import filter_atlas_data

import fastavro

stamp_parser = reqparse.RequestParser()
stamp_parser.add_argument("oid", type=str, help="Object ID", default=None)
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
avro_parser.add_argument("oid", type=str, help="Object ID", default=None)
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
    @set_permissions_decorator(["admin", "basic_user"])
    @check_permissions_decorator
    def get(self):
        args = stamp_parser.parse_args()
        candid = args["candid"]
        survey_id = args["survey_id"]
        file_type = args["type"]
        format = args["format"]
        oid = args["oid"]

        stamp_params = self.get_stamp(
            candid=candid,
            survey_id=survey_id,
            file_type=file_type,
            format=format,
            oid=oid,
        )
        if stamp_params:
            return send_file(
                stamp_params["file"],
                mimetype=stamp_params["mimetype"],
                download_name=stamp_params["download_name"],
                as_attachment=stamp_params["as_attachment"],
            )

    def get_stamp(self, candid, survey_id, file_type, format, oid=None):
        # Search in s3
        try:
            data = s3_searcher.get_file_from_s3(candid, survey_id)
            data = next(fastavro.reader(data))
            data = utils.get_stamp_type(data, file_type)
            stamp_file, mimetype, fname = utils.format_stamp(
                data, format, oid, candid, file_type
            )
            app.logger.info(f"[HIT] AVRO {candid} found in S3.")
            return {
                "file": stamp_file,
                "mimetype": mimetype,
                "download_name": fname,
                "as_attachment": True,
            }
        except FileNotFoundError:
            app.logger.info(f"[MISS] AVRO {candid} not found in S3.")

        if survey_id == "ztf":
            # Search in MARS
            try:
                avro_io = mars_searcher.get_file_from_mars(oid, int(candid))
                data = next(fastavro.reader(avro_io))
                stamp_data = utils.get_stamp_type(data, file_type)
            except Exception as e:
                app.logger.info(
                    f"[MISS] AVRO {candid} could not be retrieved from MARS."
                )
                raise NotFound("AVRO not found")

            # Upload to S3 from MARS
            try:
                avro_io.seek(0)
                app.logger.info(
                    f"[HIT] AVRO {candid} found in MARS. Uploading from MARS to S3"
                )
                reverse_candid = utils.reverse_candid(candid)
                file_name = "{}.avro".format(reverse_candid)
                s3_searcher.upload_file(avro_io, file_name, survey_id)
                stamp_file, mimetype, fname = utils.format_stamp(
                    stamp_data, format, oid, candid, file_type
                )
                return {
                    "file": stamp_file,
                    "mimetype": mimetype,
                    "download_name": fname,
                    "as_attachment": True,
                }
            except Exception as e:
                app.logger.info("Could not upload file to S3")
                raise e


@api.route("/get_avro_info")
@api.response(200, "Success")
@api.response(404, "AVRO not found")
class GetAVROInfoResource(Resource):
    @api.expect(avro_parser)
    @set_permissions_decorator(["admin", "basic_user"])
    @check_permissions_decorator
    def get(self):
        args = avro_parser.parse_args()
        candid = args["candid"]
        survey_id = args["survey_id"]
        oid = args["oid"]

        avro_data = self.get_avro(candid=candid, survey_id=survey_id, oid=oid)

        if avro_data:
            return avro_data

    @filter_atlas_data(filter_name="filter_atlas_avro", arg_key="survey_id")
    def get_avro(self, candid, survey_id, oid=None):
        try:
            data = s3_searcher.get_file_from_s3(candid, survey_id)
            data = next(fastavro.reader(data))
            if survey_id == "ztf":
                del data["cutoutTemplate"]
            del data["cutoutScience"]
            del data["cutoutDifference"]
            app.logger.info(f"[HIT] AVRO {candid} found in S3.")
            data["candidate"]["candid"] = str(data["candidate"]["candid"])
            return jsonify(data)
        except FileNotFoundError:
            app.logger.info(f"[MISS] AVRO {candid} not found in S3.")

        if survey_id == "ztf":
            try:
                avro_io = mars_searcher.get_file_from_mars(oid, int(candid))
                data = next(fastavro.reader(avro_io))
                del data["cutoutScience"]
                del data["cutoutTemplate"]
                del data["cutoutDifference"]
            except Exception as e:
                app.logger.info(
                    f"[MISS] AVRO {candid} could not be retrieved from MARS."
                )
                app.logger.error(f"Error: {e}")
                raise NotFound("AVRO not found")
            try:
                avro_io.seek(0)
                app.logger.info("Uploading Avro from MARS to S3")
                reverse_candid = utils.reverse_candid(candid)
                file_name = "{}.avro".format(reverse_candid)
                s3_searcher.upload_file(avro_io, file_name, survey_id)
                data["candidate"]["candid"] = str(data["candidate"]["candid"])
                return jsonify(data)
            except Exception as e:
                raise e


@api.route("/get_avro")
@api.response(200, "Success")
@api.response(404, "AVRO not found")
class GetAVROResource(Resource):
    @api.expect(avro_parser)
    @set_permissions_decorator(["admin", "basic_user"])
    @check_permissions_decorator
    @api.expect(avro_parser)
    def get(self):
        args = avro_parser.parse_args()
        candid = args["candid"]
        survey_id = args["survey_id"]
        oid = args["oid"]

        avro_params = self.get_avro(candid=candid, survey_id=survey_id, oid=oid)
        if avro_params:
            return send_file(
                avro_params["file"],
                mimetype=avro_params["mimetype"],
                download_name=avro_params["download_name"],
                as_attachment=avro_params["as_attachment"],
            )

    @filter_atlas_data(filter_name="filter_atlas_avro", arg_key="survey_id")
    def get_avro(self, candid, survey_id, oid=None):
        try:
            data = s3_searcher.get_file_from_s3(candid, survey_id)
            fname = f"{candid}.avro"
            app.logger.info(f"[HIT] AVRO {candid} found in S3")
            return {
                "file": data,
                "mimetype": "app/avro+binary",
                "download_name": fname,
                "as_attachment": True,
            }
        except FileNotFoundError:
            app.logger.info(f"AVRO {candid} not found in S3.")

        if survey_id == "ztf":
            try:
                avro_io = mars_searcher.get_file_from_mars(oid, int(candid))
                output = io.BytesIO(avro_io.read())
                app.logger.info(f"[HIT] AVRO {candid} found in MARS")
            except Exception as e:
                app.logger.info("File could not be retreived from MARS.")
                app.logger.error(f"Error: {e}")
                raise NotFound("AVRO not found")
            try:
                avro_io.seek(0)
                app.logger.info("Uploading Avro from MARS to S3")
                reverse_candid = utils.reverse_candid(candid)
                file_name = "{}.avro".format(reverse_candid)
                s3_searcher.upload_file(avro_io, file_name, survey_id)
                file_name = f"{candid}.avro"
                return {
                    "file": output,
                    "mimetype": "app/avro+binary",
                    "download_name": file_name,
                    "as_attachment": True,
                }
            except Exception as e:
                raise e
