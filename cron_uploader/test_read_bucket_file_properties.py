#!/usr/bin/env python3

"""

This script reads data in the incoming bucket and does the following:

1. Convert the JSON to SQL and add row to database for the dataset
2. Move h5ad file into web-accessible position

Test commands:

export PYTHONPATH=$HOME/git/gEAR/lib:$PYTHONPATH
./nemo_gcloud_processor.py


"""

import argparse, json, os, sys
import datetime
import shutil
import subprocess
import uuid

from google.cloud import storage
GCLOUD_PROJECT = 'nemo-analytics'
GCLOUD_BUCKET = 'nemo-analytics-incoming'

def main():
    parser = argparse.ArgumentParser( description='NeMO data processor for gEAR')
    args = parser.parse_args()

    sclient = storage.Client(project=GCLOUD_PROJECT)
    bucket = storage.bucket.Bucket(client=sclient, name=GCLOUD_BUCKET)

    blob = bucket.get_blob('8eb9cd46-a5c8-40e6-983b-8809eb1201cc.h5ad')

    #reload call is required for some attributes to be populated
    # https://news.ycombinator.com/item?id=17516153
    blob.reload()
    blob_metadata(blob)
    

    
def blob_metadata(blob):
    """Prints out a blob's metadata."""

    print("Blob: {}".format(blob.name))
    print("Bucket: {}".format(blob.bucket.name))
    print("Storage class: {}".format(blob.storage_class))
    print("ID: {}".format(blob.id))
    print("Size: {} bytes".format(blob.size))
    print("Updated: {}".format(blob.updated))
    print("Generation: {}".format(blob.generation))
    print("Metageneration: {}".format(blob.metageneration))
    print("Etag: {}".format(blob.etag))
    print("Owner: {}".format(blob.owner))
    print("Component count: {}".format(blob.component_count))
    print("Crc32c: {}".format(blob.crc32c))
    print("md5_hash: {}".format(blob.md5_hash))
    print("Cache-control: {}".format(blob.cache_control))
    print("Content-type: {}".format(blob.content_type))
    print("Content-disposition: {}".format(blob.content_disposition))
    print("Content-encoding: {}".format(blob.content_encoding))
    print("Content-language: {}".format(blob.content_language))
    print("Metadata: {}".format(blob.metadata))
    print("Temporary hold: ", "enabled" if blob.temporary_hold else "disabled")
    print(
        "Event based hold: ",
        "enabled" if blob.event_based_hold else "disabled",
    )
    if blob.retention_expiration_time:
        print(
            "retentionExpirationTime: {}".format(
                blob.retention_expiration_time
            )
        )

if __name__ == '__main__':
    main()







