# [ALeRCE](http://alerce.science) AVRO Service
[![Documentation Status](https://readthedocs.org/projects/alerceapi/badge/?version=latest)](https://alerceapi.readthedocs.io/en/latest/avro.html?badge=latest)

ALeRCE Stamp Service stores and gives access to AVRO files, Stamps and Metadata for ZTF and other surveys.


## Documentation
The current documentation can be found [here](https://alerceapi.readthedocs.io/en/latest/avro.html).

## How the data is stored

### On Amazon S3 storage

The index for objects in S3 is in the form `<reverse_candid>.avro`. That means that if an alert has `candid = 123`, the reverse candid would be `reverse_candid = 321`.

## How a Stamp is transformed into *png*.

Using the straightforward approach to generate an image from the stamp can give a low contrast image.

To get a better image we scale the data with a min/max threshold.

For the max threshold we select a window of 2 pixels around the central object and get the max value of that window.

![transform process](doc/transform.png)

As for the min threshold we use the following

<p align="center">
  <img src="doc/min.jpg">
</p>

These thresholds are not applied to the `difference` stamps.

## Deploying Stamp Service

The stamp service is deployed as a docker container, to build the image run:
```
  docker build -t stamp_service .
```

Then the container can be created with
```
  docker run --name stamp_service -p 8087:8087 \
              stamp_service
```

Configuration env variables for the container are:

| Variable name            | Description                             | Default | Required |
|--------------------------|-----------------------------------------|---------|----------|
| ZTF_BUCKET_NAME          | Name of the S3 bucket with ZTF AVROs    |         | &check;  |
| ATLAS_BUCKET_NAME        | Name of the S3 bucket with ATLAS AVROs  |         | &check;  |
| MARS_URL                 | URL for the MARS API                    |         | &check;  |
| APP_BIND                 | Gunicorn bind address                   | 0.0.0.0 |          |
| APP_PORT                 | Gunicorn port                           | 8087    |          |
| APP_WORKERS              | Gunicorn num of workers                 | 6       |          |
| LOG_LEVEL                | LOG level                               | INFO    |          |
| prometheus_multiproc_dir | Directory for metrics                   |         | &check;  |

