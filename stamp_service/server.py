from envyaml import EnvYAML
from fastapi import FastAPI
from starlette_prometheus import metrics, PrometheusMiddleware
from ralidator_fastapi.ralidator_fastapi import RalidatorStarlette

from stamp_service.resources import add_get_stamp
from stamp_service.search import MARSSearcher, S3Searcher


def create_app(config_path):
    application = FastAPI()
    config_dict = EnvYAML(config_path)
    server_settings = config_dict["SERVER_SETTINGS"]
    ralidator_settings = config_dict["RALIDATOR_SETTINGS"]
    filters_map = {}
    # application.add_middleware(
    #     RalidatorStarlette,
    #     config=ralidator_settings,
    #     filters_map=filters_map,
    #     ignore_paths=["/metrics", "/docs"],
    # )
    s3_searcher = S3Searcher()
    s3_searcher.init(bucket_config=server_settings["SURVEY_SETTINGS"])
    mars_searcher = MARSSearcher()
    mars_searcher.init(config_dict["SERVER_SETTINGS"]["mars_url"])
    application.add_middleware(PrometheusMiddleware)
    application.add_route("/metrics", metrics)
    add_get_stamp(application, s3_searcher, mars_searcher)

    return application
