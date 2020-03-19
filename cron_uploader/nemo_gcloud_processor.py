#!/usr/bin/env python3

"""

This script reads data in the incoming bucket and does the following:

1. Convert the JSON to SQL and add row to database for the dataset
2. Move h5ad file into web-accessible position

Test commands:

export PYTHONPATH=$HOME/git/gEAR/lib:$PYTHONPATH
./nemo_gcloud_processor.py


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
destination_path = '/home/jorvis/git/gEAR/www/datasets'
dataset_owner_id = 487

def main():
    parser = argparse.ArgumentParser( description='NeMO data processor for gEAR')
    parser.add_argument('-s', '--skip_ids', type=str, required=False, help='Comma-separated list of IDs to skip while processing' )
    args = parser.parse_args()

    ids_to_skip = []

    if args.skip_ids:
        ids_to_skip = args.skip_ids.split(',')

    sclient = storage.Client(project=gcloud_project)
    bucket = storage.bucket.Bucket(client=sclient, name=gcloud_bucket)

    h5s = get_bucket_h5_list(sclient, bucket)

    for h5 in h5s:
        dataset_id = h5.replace('.h5ad', '')

        if dataset_id in ids_to_skip:
            continue
        
        download_data_for_processing(bucket, dataset_id)

        metadata_path = "{0}/{1}.json".format(processing_directory, dataset_id)
        h5ad_path = "{0}/{1}.h5ad".format(processing_directory, dataset_id)
        
        metadata = Metadata(file_path=metadata_path)
        metadata.add_field_value('dataset_uid', dataset_id)
        metadata.add_field_value('owner_id', dataset_owner_id)
        metadata.add_field_value('schematic_image', '')
        metadata.add_field_value('share_uid', str(uuid.uuid4()))
        metadata.add_field_value('default_plot_type', '')
        metadata.add_field_value('is_public', '1')

        # hack for annotation source currently until NCBI is supported
        annot_release = metadata.get_field_value('annotation_release_number')
        if isinstance(annot_release, dict):
            if annot_release['value'].startswith('hg'):
                annot_release['value'] = 92
        else:
            if annot_release.startswith('hg'):
                annot_release = 92

        metadata.save_to_mysql(status='completed')

        # place the files where they go on the file system to be live in gEAR
        shutil.move(metadata_path, "{0}/".format(destination_path))
        shutil.move(h5ad_path, "{0}/".format(destination_path))

        # remove files from bucket
        for extension in ['h5ad', 'json']:
            blob = bucket.blob("{0}.{1}".format(dataset_id, extension))
            blob.delete()
    

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







