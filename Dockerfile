FROM python:3.9

RUN apt-get update
RUN apt-get -y install libsnappy-dev

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt && pip install flask gunicorn

COPY . /app
EXPOSE 8087
ENV APP_BIND="0.0.0.0"
ENV APP_PORT="8087"
ENV APP_WORKERS=1
ENV APP_THREADS=3

CMD ["/bin/bash","scripts/entrypoint.sh"]
