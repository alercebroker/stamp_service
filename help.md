build:
docker build --tag flask_gunicorn_stamps_server .
run:
sudo docker run -p 8087:8087 -v /mnt/stamps:/mnt/stamps flask_gunicorn_stamps_server
run deatached:
sudo docker run -d -p 8087:8087 -v /mnt/stamps:/mnt/stamps flask_gunicorn_stamps_server
