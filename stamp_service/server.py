from flask import Flask
from flask_cors import CORS
from .extensions import prometheus_metrics
from .callbacks import after_request, before_request
import os
import logging


def create_app(config):
    application = Flask(__name__)
    application.config.from_object(config)
    CORS(application)
    # Check if app run trough gunicorn
    is_gunicorn = "gunicorn" in os.environ.get("SERVER_SOFTWARE", "")

    if is_gunicorn:
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

        s3_searcher.init(bucket_name=os.environ["BUCKET_NAME"])
        mars_searcher.init(mars_url=os.environ["MARS_URL"])

        from .resources import api

        api.init_app(application)
    return application
