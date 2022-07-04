from prometheus_flask_exporter.multiprocess import GunicornInternalPrometheusMetrics
from prometheus_flask_exporter import PrometheusMetrics

def set_prometheus_metrics(app):
    from flask import current_app

    is_gunicorn = "gunicorn" in current_app.config["SERVER_SETTINGS"]["server_software"]
    if is_gunicorn:
        return GunicornInternalPrometheusMetrics.for_app_factory()
    else:
        return PrometheusMetrics.for_app_factory()
