from prometheus_flask_exporter.multiprocess import GunicornInternalPrometheusMetrics
from prometheus_flask_exporter import PrometheusMetrics


def set_prometheus_metrics(app):
    is_gunicorn = "gunicorn" in app.config["SERVER_SETTINGS"]["server_software"]
    if is_gunicorn:
        prometheus_metrics = GunicornInternalPrometheusMetrics.for_app_factory()
    else:
        prometheus_metrics = PrometheusMetrics.for_app_factory()
    
    prometheus_metrics.init_app(app)
