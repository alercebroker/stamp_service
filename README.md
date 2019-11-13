# [ALeRCE](http://alerce.science) AVRO Service

ALeRCE Stamp Service stores and gives access to AVRO files, Stamps and Metadata for ZTF and other surveys.


## Documentation
The current documentation can be found [here](https://alerceapi.readthedocs.io/en/latest/avro.html).

## How it stores the data?

The stamp service uses a 8 folder configuration to store the data.

By default is mounted on the following directories `/mnt/stamps/[0-7]`.

Each time a new avro is uploaded a hash function is calculated over the object id `oid` of the file, using that hash value it is stored in one of the 8 disks, this gives a uniform distribution of data between the disks.

## How the stamp is transformed into *png*.

Using the straightforward approach to generate an image from the stamp can gives an low contrast image.

To get a better image we select a window around the central object and get the max value of that window, this gives a better contrast but with some oversaturated stamps.


## Deploying Stamp Service

```
  docker build -t stamp_service .
```
