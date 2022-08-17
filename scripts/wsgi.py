import sys
import logging

sys.path.append("..")
from stamp_service.server import create_app


if __name__ == "__main__":
    create_app('../config.yml').run(debug=True)
