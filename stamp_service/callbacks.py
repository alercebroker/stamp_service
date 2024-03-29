import traceback
from time import time

from flask import request, g


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


def exceptions(e, logger):
    tb = traceback.format_exc()
    logger.error(
        "%s %s %s %s ERROR\n%s",
        request.remote_addr,
        request.method,
        request.scheme,
        request.full_path,
        tb,
    )
    return e.status_code
