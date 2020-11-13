import sys
sys.path.append("..")
from stamp_service.server import create_app


if __name__ == "__main__":
    create_app({}).run(debug=True)
