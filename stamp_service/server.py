from flask import Flask
from flask_cors import CORS
from .extensions import prometheus_metrics
from .callbacks import after_request, before_request
import os
import logging


def init_logging():
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)s %(module)s %(name)s.%(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def create_app(config):
    init_logging()
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
        from .search import s3_searcher, mars_searcher, disc_searcher

        s3_searcher.init(bucket_name=os.environ["BUCKET_NAME"])
        mars_searcher.init(mars_url=os.environ["MARS_URL"])
        if os.getenv("USE_DISK", False):
            disc_searcher.init(
                root_path=os.environ["ROOT_PATH"], ndisk=os.environ["NDISK"]
            )
        else:
            disc_searcher = None

        from .resources import api

        api.init_app(application)
    return application
