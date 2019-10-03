#!/usr/bin/env python3

"""

This script reads data in the incoming bucket and does the following:

1. Convert the JSON to SQL and add row to database for the dataset
2. Move h5ad file into web-accessible position

Test commands:

export PYTHONPATH=$HOME/git/gEAR/lib:$PYTHONPATH



"""

import argparse
import os
import json
from gear.metadata import Metadata
import sys
import shutil
import uuid

from google.cloud import storage
gcloud_project = 'nemo-analytics'
gcloud_bucket = 'nemo-analytics-incoming'
processing_directory = '/tmp'
dataset_owner_id = 487

def main():
    parser = argparse.ArgumentParser( description='NeMO data processor for gEAR')

    #parser.add_argument('-ilb', '--input_log_base', type=str, required=True, help='Path to the base directory where the logs are found' )
    #parser.add_argument('-ob', '--output_base', type=str, required=True, help='Path to a local output directory where files can be written while processing' )
    args = parser.parse_args()

    sclient = storage.Client(project=gcloud_project)
    bucket = storage.bucket.Bucket(client=sclient, name=gcloud_bucket)

    h5s = get_bucket_h5_list(sclient, bucket)

    for h5 in h5s:
        dataset_id = h5.replace('.h5ad', '')
        download_data_for_processing(bucket, dataset_id)
        
        metadata = Metadata(file_path="{0}/{1}.json".format(processing_directory, dataset_id))
        metadata.add_field_value(field='dataset_uid', value=dataset_id)
        metadata.add_field_value(field='owner_id', value=dataset_owner_id)
        metadata.add_field_value(field='schematic_image', value='')
        metadata.add_field_value(field='share_uid', value=str(uuid.uuid4()))
        metadata.add_field_value(field='default_plot_type', value='')
        metadata.save_to_mysql(status='completed')

        
    

def download_data_for_processing(bucket, dataset_id):
    for extension in ['h5ad', 'json']:
        path = "{0}/{1}.{2}".format(processing_directory, dataset_id, extension)
        blob = bucket.blob("{0}.{1}".format(dataset_id, extension))
        log('INFO', "Downloading file: {0}".format("{0}.{1}".format(dataset_id, extension)))
        blob.download_to_filename(path)

def get_bucket_h5_list(sclient, bucket):
    h5s = list()

    for blob in sclient.list_blobs(bucket):
        if blob.name.endswith('.h5ad'):
            h5s.append(blob.name)

    return h5s
    
def log(level, msg):
    print("{0}: {1}".format(level, msg),  flush=True)

def run_command(cmd):
    log("INFO", "Running command: {0}".format(cmd))
    return_code = subprocess.call(cmd, shell=True)
    if return_code != 0:
       raise Exception("ERROR: [{2}] Return code {0} when running the following command: {1}".format(return_code, cmd, datetime.datetime.now()))

if __name__ == '__main__':
    main()







