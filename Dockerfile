FROM python:3.6.8

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt && pip install flask gunicorn

COPY . /app
EXPOSE 8087
ENV PYTHONHASHSEED=0
ENV AVRO_ROOT=/mnt/stamps
ENV APP_BIND="0.0.0.0"
ENV APP_PORT="8087"
ENV APP_WORKERS=6

RUN mkdir /mnt/stamps

WORKDIR /app/scripts
CMD ["/bin/bash","entrypoint.sh"]
