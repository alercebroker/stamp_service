from flask import Flask
from flask_cors import CORS
from .callbacks import after_request, before_request
from .utils import set_logger
from .extensions import set_prometheus_metrics, ralidator
from envyaml import EnvYAML


def create_app(config_path):
    application = Flask(__name__)
    config_dict = EnvYAML(config_path)
    application.config["SERVER_SETTINGS"] = config_dict["SERVER_SETTINGS"]
    application.config["RALIDATOR_SETTINGS"] = config_dict["RALIDATOR_SETTINGS"]
    application.config["FILTERS_MAP"] = {}
    CORS(application)

    ralidator.init_app(application)
    set_prometheus_metrics(application)
    set_logger(application)

    @application.before_request
    def beforerequest():
        before_request()

    @application.after_request
    def afterrequest(response):
        return after_request(response, application.logger)

    with application.app_context():
        from .search import s3_searcher, mars_searcher

        s3_searcher.init(application.config["SERVER_SETTINGS"]["SURVEY_SETTINGS"])
        mars_searcher.init(mars_url=application.config["SERVER_SETTINGS"]["mars_url"])

        from .resources import api

        api.init_app(application)
    return application
