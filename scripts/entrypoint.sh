#!/bin/bash

gunicorn -b $APP_BIND:$APP_PORT -w $APP_WORKERS wsgi
