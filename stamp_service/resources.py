import io
from . import utils
from ralidator_fastapi.decorators import (
    set_permissions_decorator,
    check_permissions_decorator,
)

# from .filters import filter_atlas_data
import fastavro
import logging
from starlette.requests import Request
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse


def add_get_stamp(api: FastAPI, s3_searcher, mars_searcher):
    @api.get("/get_stamp")
    # @set_permissions_decorator(["admin", "basic_user"])
    # @check_permissions_decorator
    async def get_stamp(
        request: Request, candid: str, survey_id: str, type: str, format: str
    ):
        stamp_params = _get_stamp(
            candid=candid,
            survey_id=survey_id,
            file_type=type,
            format=format,
        )
        if stamp_params:

            def iterfile():  #
                yield from stamp_params["file"]

            return StreamingResponse(
                iterfile(),
                media_type=stamp_params["mimetype"],
            )

    def _get_stamp(candid, survey_id, file_type, format, oid=None):
        # Search in s3
        try:
            data = s3_searcher.get_file_from_s3(candid, survey_id)
            data = next(fastavro.reader(data))
            data = utils.get_stamp_type(data, file_type)
            stamp_file, mimetype, fname = utils.format_stamp(
                data, format, oid, candid, file_type
            )
            logging.info(f"[HIT] AVRO {candid} found in S3.")
            return {
                "file": stamp_file,
                "mimetype": mimetype,
                "download_name": fname,
            }
        except FileNotFoundError:
            logging.info(f"[MISS] AVRO {candid} not found in S3.")

        if survey_id == "ztf":
            # Search in MARS
            try:
                avro_io = mars_searcher.get_file_from_mars(oid, int(candid))
                data = next(fastavro.reader(avro_io))
                stamp_data = utils.get_stamp_type(data, file_type)
            except Exception as e:
                logging.info(f"[MISS] AVRO {candid} could not be retrieved from MARS.")
                raise HTTPException(status_code=404, detail="AVRO not found")

            # Upload to S3 from MARS
            try:
                avro_io.seek(0)
                logging.info(
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
                logging.info("Could not upload file to S3")
                raise e


# @api.route("/get_avro_info")
# @api.response(200, "Success")
# @api.response(404, "AVRO not found")
# class GetAVROInfoResource(Resource):
#     @api.expect(avro_parser)
#     @set_permissions_decorator(["admin", "basic_user"])
#     @check_permissions_decorator
#     def get(self):
#         args = avro_parser.parse_args()
#         candid = args["candid"]
#         survey_id = args["survey_id"]
#         oid = args["oid"]

#         avro_data = self.get_avro(candid=candid, survey_id=survey_id, oid=oid)

#         if avro_data:
#             return avro_data

#     @filter_atlas_data(filter_name="filter_atlas_avro", arg_key="survey_id")
#     def get_avro(self, candid, survey_id, oid=None):
#         try:
#             data = s3_searcher.get_file_from_s3(candid, survey_id)
#             data = next(fastavro.reader(data))
#             if survey_id == "ztf":
#                 del data["cutoutTemplate"]
#             del data["cutoutScience"]
#             del data["cutoutDifference"]
#             app.logger.info(f"[HIT] AVRO {candid} found in S3.")
#             data["candidate"]["candid"] = str(data["candidate"]["candid"])
#             return jsonify(data)
#         except FileNotFoundError:
#             app.logger.info(f"[MISS] AVRO {candid} not found in S3.")

#         if survey_id == "ztf":
#             try:
#                 avro_io = mars_searcher.get_file_from_mars(oid, int(candid))
#                 data = next(fastavro.reader(avro_io))
#                 del data["cutoutScience"]
#                 del data["cutoutTemplate"]
#                 del data["cutoutDifference"]
#             except Exception as e:
#                 app.logger.info(
#                     f"[MISS] AVRO {candid} could not be retrieved from MARS."
#                 )
#                 app.logger.error(f"Error: {e}")
#                 raise NotFound("AVRO not found")
#             try:
#                 avro_io.seek(0)
#                 app.logger.info("Uploading Avro from MARS to S3")
#                 reverse_candid = utils.reverse_candid(candid)
#                 file_name = "{}.avro".format(reverse_candid)
#                 s3_searcher.upload_file(avro_io, file_name, survey_id)
#                 data["candidate"]["candid"] = str(data["candidate"]["candid"])
#                 return jsonify(data)
#             except Exception as e:
#                 raise e


# @api.route("/get_avro")
# @api.response(200, "Success")
# @api.response(404, "AVRO not found")
# class GetAVROResource(Resource):
#     @api.expect(avro_parser)
#     @set_permissions_decorator(["admin", "basic_user"])
#     @check_permissions_decorator
#     @api.expect(avro_parser)
#     def get(self):
#         args = avro_parser.parse_args()
#         candid = args["candid"]
#         survey_id = args["survey_id"]
#         oid = args["oid"]

#         avro_params = self.get_avro(candid=candid, survey_id=survey_id, oid=oid)
#         if avro_params:
#             return send_file(
#                 avro_params["file"],
#                 mimetype=avro_params["mimetype"],
#                 download_name=avro_params["download_name"],
#                 as_attachment=avro_params["as_attachment"],
#             )

#     @filter_atlas_data(filter_name="filter_atlas_avro", arg_key="survey_id")
#     def get_avro(self, candid, survey_id, oid=None):
#         try:
#             data = s3_searcher.get_file_from_s3(candid, survey_id)
#             fname = f"{candid}.avro"
#             app.logger.info(f"[HIT] AVRO {candid} found in S3")
#             return {
#                 "file": data,
#                 "mimetype": "app/avro+binary",
#                 "download_name": fname,
#                 "as_attachment": True,
#             }
#         except FileNotFoundError:
#             app.logger.info(f"AVRO {candid} not found in S3.")

#         if survey_id == "ztf":
#             try:
#                 avro_io = mars_searcher.get_file_from_mars(oid, int(candid))
#                 output = io.BytesIO(avro_io.read())
#                 app.logger.info(f"[HIT] AVRO {candid} found in MARS")
#             except Exception as e:
#                 app.logger.info("File could not be retreived from MARS.")
#                 app.logger.error(f"Error: {e}")
#                 raise NotFound("AVRO not found")
#             try:
#                 avro_io.seek(0)
#                 app.logger.info("Uploading Avro from MARS to S3")
#                 reverse_candid = utils.reverse_candid(candid)
#                 file_name = "{}.avro".format(reverse_candid)
#                 s3_searcher.upload_file(avro_io, file_name, survey_id)
#                 file_name = f"{candid}.avro"
#                 return {
#                     "file": output,
#                     "mimetype": "app/avro+binary",
#                     "download_name": file_name,
#                     "as_attachment": True,
#                 }
#             except Exception as e:
#                 raise e
