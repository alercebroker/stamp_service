#!/bin/bash

gunicorn -b $APP_BIND:$APP_PORT -w $APP_WORKERS --threads=$APP_THREADS "stamp_service.server:create_app({})"
