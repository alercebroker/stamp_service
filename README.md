# [ALeRCE](http://alerce.science) AVRO Service
[![Documentation Status](https://readthedocs.org/projects/alerceapi/badge/?version=latest)](https://alerceapi.readthedocs.io/en/latest/?badge=latest)

ALeRCE Stamp Service stores and gives access to AVRO files, Stamps and Metadata for ZTF and other surveys.


## Documentation
The current documentation can be found [here](https://alerceapi.readthedocs.io/en/latest/avro.html).

## How it stores the data?

The stamp service uses a 8 folder configuration to store the data.

By default is mounted on the following directories `/mnt/stamps/[0-7]`.

Each time a new avro is uploaded a hash function is calculated over the object id `oid` of the file, using that hash value it is stored in one of the 8 disks, this gives a uniform distribution of data between the disks.

## How the stamp is transformed into *png*.

Using the straightforward approach to generate an image from the stamp can gives an low contrast image.

![transform process](doc/transform.png)

To get a better image we select a window around the central object and get the max value of that window, this gives a better contrast but with some oversaturated stamps.


## Deploying Stamp Service


The stamp service is deployed as a docker container, to build the image run:
```
  docker build -t stamp_service .
```

Then the container can be created with
```
  docker run --name stamp_service -p 8087:8087 \
             -v <disks_path>:/mnt/stamps
              stamp_service
```

Some other configurations for the container are:

```
PYTHONHASHSEED    Seed for Hash calculation (default 0)
AVRO_ROOT         Location of disks         (default /mnt/stamps)
APP_BIND          Gunicorn bind address     (default 0.0.0.0)
APP_PORT          Gunicorn port             (default 8087)
APP_WORKERS       Gunicorn num of workers   (default 6)
```
