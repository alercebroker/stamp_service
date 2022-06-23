from flask import request, g
from time import time


def before_request():
    g.time = time()


def after_request(response, logger):
    if request.full_path == "/metrics?":
        return response
    elapsed = time() - g.pop("time")
    logger.info(
        "%s %s %s %s %s time:%s seconds",
        request.remote_addr,
        request.method,
        request.scheme,
        request.full_path,
        response.status,
        elapsed,
    )
    return response
