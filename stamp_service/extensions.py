from prometheus_flask_exporter.multiprocess import GunicornInternalPrometheusMetrics
from prometheus_flask_exporter import PrometheusMetrics
import os
from flask import current_app

is_gunicorn = "gunicorn" in current_app.config["SERVER_SETTINGS"]["server_software"]
if is_gunicorn:
    prometheus_metrics = GunicornInternalPrometheusMetrics.for_app_factory()
else:
    prometheus_metrics = PrometheusMetrics.for_app_factory()
