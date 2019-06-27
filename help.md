build:
docker build --tag flask_gunicorn_stamps_server .
run:
docker run -p 8087:8087 \
-v /mnt/stamps/0:/mnt/stamps/0 \
-v /mnt/stamps/0:/mnt/stamps/0 \
-v /mnt/stamps/0:/mnt/stamps/0 \
-v /mnt/stamps/0:/mnt/stamps/0 \
-v /mnt/stamps/0:/mnt/stamps/0 \
-v /mnt/stamps/0:/mnt/stamps/0 \
-v /mnt/stamps/0:/mnt/stamps/0 \
-v /mnt/stamps/0:/mnt/stamps/0 \
flask_gunicorn_stamps_server
run deatached:
docker run --detach -p 8087:8087 \
-v /mnt/stamps/0:/mnt/stamps/0 \
-v /mnt/stamps/0:/mnt/stamps/0 \
-v /mnt/stamps/0:/mnt/stamps/0 \
-v /mnt/stamps/0:/mnt/stamps/0 \
-v /mnt/stamps/0:/mnt/stamps/0 \
-v /mnt/stamps/0:/mnt/stamps/0 \
-v /mnt/stamps/0:/mnt/stamps/0 \
-v /mnt/stamps/0:/mnt/stamps/0 \
flask_gunicorn_stamps_server