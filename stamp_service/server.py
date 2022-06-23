from flask import Flask
from flask_cors import CORS
from .callbacks import after_request, before_request
from .utils import get_configuration_object
import os
import logging


def create_app(config_path):
    application = Flask(__name__)
    config_dict = get_configuration_object(config_path)
    application.config["SERVER_SETTINGS"] = config_dict
    CORS(application)
    # Check if app run trough gunicorn
    is_gunicorn = "gunicorn" in application.config["SERVER_SETTINGS"]["server_software"]
    
    if is_gunicorn:
        from .extensions import prometheus_metrics
        prometheus_metrics.init_app(application)
        gunicorn_logger = logging.getLogger("gunicorn.error")
        application.logger.handlers = gunicorn_logger.handlers
        application.logger.setLevel(gunicorn_logger.level)

    @application.before_request
    def beforerequest():
        before_request()

    @application.after_request
    def afterrequest(response):
        return after_request(response, application.logger)

    with application.app_context():
        from .search import s3_searcher, mars_searcher

        s3_searcher.init(application.config["SERVER_SETTINGS"]["SURVEY_SETTINGS"]["ztf"]["bucket"])
        mars_searcher.init(mars_url=application.config["SERVER_SETTINGS"]["mars_url"])

        from .resources import api

        api.init_app(application)
    return application
