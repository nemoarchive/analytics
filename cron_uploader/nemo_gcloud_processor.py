#!/opt/bin/python3

"""

This script reads data in the incoming bucket and does the following:

1. Convert the JSON to SQL and add row to database for the dataset
2. Move h5ad file into web-accessible position

Test commands:

export PYTHONPATH=$HOME/git/gEAR/lib:$PYTHONPATH
./nemo_gcloud_processor.py

Depends on a .conf.ini file in the same directory as this script with the following entries:

[paths]
dataset_processing_log=/home/jorvis/logs/nemo_gcloud_processor.log
gear_dir = /home/jorvis/git/gEAR
processing_dir = /tmp
dataset_dest = /home/jorvis/git/gEAR/www/datasets

[metadata]
dataset_owner_id = 1
annot_release_num = 92

Other entries can be there, but these are all that this script requires.

"""

import argparse, json, os, sys
import datetime
import shutil
import subprocess
import uuid

import configparser
conf_loc = os.path.join(os.path.dirname(__file__), '.conf.ini')
if not os.path.isfile(conf_loc):
    sys.exit("Config file could not be found at {}".format(conf_loc))
config = configparser.ConfigParser()
config.read(conf_loc)

# Read gEAR library modules
sys.path.append(os.path.join(config.get("paths", "gear_dir"), "lib"))
from gear.metadata import Metadata

from google.cloud import storage
GCLOUD_PROJECT = config.get("gcloud", "project")
GCLOUD_BUCKET = config.get("gcloud", "bucket")

PROCESSING_DIRECTORY = config.get("paths", "processing_dir")
DESTINATION_PATH = config.get("paths", "dataset_dest")
DATASET_OWNER_ID = config.get("metadata", "dataset_owner_id")
DEFAULT_ANNOT_RELEASE_NUM = config.get("metadata", "annot_release_num")
LOG_FH = open(config.get("paths", "dataset_processing_log"), 'a')


def main():
    parser = argparse.ArgumentParser( description='NeMO data processor for gEAR')
    parser.add_argument('-s', '--skip_ids', type=str, required=False, help='Comma-separated list of IDs to skip while processing' )
    args = parser.parse_args()

    ids_to_skip = []

    if args.skip_ids:
        ids_to_skip = args.skip_ids.split(',')

    sclient = storage.Client(project=GCLOUD_PROJECT)
    bucket = storage.bucket.Bucket(client=sclient, name=GCLOUD_BUCKET)

    h5s = get_bucket_h5_list(sclient, bucket)
    log('INFO', "There are {0} H5AD files to process".format(len(h5s)))

    for h5 in h5s:
        dataset_id = h5.replace('.h5ad', '')

        if dataset_id in ids_to_skip:
            log("INFO: Skipping dataset_id:{0} because it is in the skip list".format(dataset_id))
            continue

        h5_blob = bucket.blob("{0}.h5ad".format(dataset_id))

        log("INFO: Started processing dataset_id:{0}".format(dataset_id))
        download_data_for_processing(bucket, dataset_id)

        metadata_path = "{0}/{1}.json".format(PROCESSING_DIRECTORY, dataset_id)
        h5ad_path = "{0}/{1}.h5ad".format(PROCESSING_DIRECTORY, dataset_id)

        log("INFO: Parsing metadata for dataset_id:{0}".format(dataset_id))
        metadata = Metadata(file_path=metadata_path)
        metadata.add_field_value('dataset_uid', dataset_id)
        metadata.add_field_value('owner_id', DATASET_OWNER_ID)
        metadata.add_field_value('schematic_image', '')
        metadata.add_field_value('share_uid', str(uuid.uuid4()))
        metadata.add_field_value('default_plot_type', '')
        metadata.add_field_value('is_public', '1')

        # Populates empty fields from GEO (if GEO GSE ID was given)
        try:
            metadata.populate_from_geo()
        except KeyError:
            log('WARN', 'Unable to process GEO ID.  Please check it and try again.')

        # hack for annotation source currently until NCBI is supported
        annot_release = metadata.get_field_value('annotation_release_number')
        if isinstance(annot_release, dict):
            if annot_release['value'].startswith('hg'):
                annot_release['value'] = DEFAULT_ANNOT_RELEASE_NUM
        else:
            if annot_release.startswith('hg'):
                annot_release = DEFAULT_ANNOT_RELEASE_NUM

        try:
            metadata.save_to_mysql(status='completed')
            log('INFO', "Saved metadata to database for dataset_id:{0}".format(dataset_id))
        except:
            log('ERROR', "Failed to save metadata to database for dataset_id:{0}".format(dataset_id))
            continue

        # place the files where they go on the file system to be live in gEAR
        try:
            shutil.move(metadata_path, "{0}/".format(DESTINATION_PATH))
            shutil.move(h5ad_path, "{0}/".format(DESTINATION_PATH))
            log('INFO', "Successfully migrated datafiles for dataset_id:{0}".format(dataset_id))
        except:
            log('ERROR', "Failed to migrate datafiles for dataset_id:{0}".format(dataset_id))
            continue

        # remove files from bucket
        for extension in ['h5ad', 'json']:
            blob = bucket.blob("{0}.{1}".format(dataset_id, extension))
            blob.delete()


def download_data_for_processing(bucket, dataset_id):
    for extension in ['h5ad', 'json']:
        path = "{0}/{1}.{2}".format(PROCESSING_DIRECTORY, dataset_id, extension)
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
    print("{0} - {1}: {2}".format(level, datetime.datetime.now(), msg), flush=True, file=LOG_FH)

def run_command(cmd):
    log("INFO", "Running command: {0}".format(cmd))
    return_code = subprocess.call(cmd, shell=True)
    if return_code != 0:
       raise Exception("ERROR: [{2}] Return code {0} when running the following command: {1}".format(return_code, cmd, datetime.datetime.now()))

if __name__ == '__main__':
    main()







