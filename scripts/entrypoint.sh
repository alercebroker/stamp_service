#!/bin/bash

gunicorn -b $APP_BIND:$APP_PORT -w $APP_WORKERS --threads=$APP_THREADS --log-level=$LOG_LEVEL "stamp_service.server:create_app({})"
