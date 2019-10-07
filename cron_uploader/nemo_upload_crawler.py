#!/usr/bin/env python3

"""

This is a script to read a log directory of expression data ready to parse, perform
validation, convert to H5AD, then push to the cloud node for import by a gEAR instance.

Test commands:

gcloud auth activate-service-account --key-file $HOME/keys/nemo-analytics__archive-file-transfer.json
export GOOGLE_APPLICATION_CREDENTIALS=$HOME/keys/nemo-analytics__archive-file-transfer.json
export PYTHONPATH=$HOME/git/gEAR/lib:$PYTHONPATH
export PATH=/usr/local/common/Python-3.7.2/bin:$PATH

# MEX
/usr/local/common/Python-3.7.2/bin/python3 ~/git/analytics/cron_uploader/nemo_upload_crawler.py -ilb /local/scratch/achatterjee/MEX_TEST/IN/ -ob  ./

# 3-tab
/usr/local/common/Python-3.7.2/bin/python3 ~/git/analytics/cron_uploader/nemo_upload_crawler.py -ilb /local/scratch/achatterjee/Test_New/ -ob ./

"""

import argparse
import os
import datetime
import uuid
#from gear.datasetuploader import FileType, DatasetUploader
#import gear.mexuploader
#from gear.metadatauploader import MetadataUploader
import pandas
import tarfile
import json
import ntpath
import csv
import itertools
from gear.metadata import Metadata
from gear.dataarchive import DataArchive
import sys
import subprocess
import shutil

from google.cloud import storage
gcloud_project = 'nemo-analytics'
gcloud_bucket = 'nemo-analytics-incoming'

def main():
    parser = argparse.ArgumentParser( description='NeMO data processor for gEAR')

    parser.add_argument('-ilb', '--input_log_base', type=str, required=True, help='Path to the base directory where the logs are found' )
    parser.add_argument('-ob', '--output_base', type=str, required=True, help='Path to a local output directory where files can be written while processing' )
    args = parser.parse_args()

    sclient = storage.Client(project=gcloud_project)
    bucket = storage.bucket.Bucket(client=sclient, name=gcloud_bucket)

    files_pending = get_datasets_to_process(args.input_log_base, args.output_base)
    for file_path in files_pending:
        log('INFO', "Processing datafile at path:{0}".format(file_path))
        dataset_id = uuid.uuid4()
        dataset_dir = extract_dataset(file_path, args.output_base)
        metadata_file_path = get_metadata_file(dataset_dir)
        metadata = Metadata(file_path=metadata_file_path)
        if metadata.validate():
            log('INFO', "Metadata file is valid: {0}".format(metadata_file_path))
            try:
                metadata_json_path = "{0}/{1}.json".format(args.output_base, dataset_id)
                metadata.write_json(file_path=metadata_json_path)
                organism_taxa = get_organism_id(metadata_file_path)
                organism_id = get_gear_organism_id(organism_taxa)
                h5_path = convert_to_h5ad(dataset_dir, dataset_id, args.output_base) 
                ensure_ensembl_index(h5_path, organism_id)
                upload_to_cloud(bucket, h5_path, metadata_json_path)
            except:
                log('ERROR', "Failed to process file:{0}".format(file_path))
        else:
            log('INFO', "Metadata file is NOT valid: {0}".format(metadata_file_path))

            
def convert_to_h5ad(dataset_dir, dataset_id, output_dir):
    """
    Input: An extracted directory containing expression data files to be converted
           to H5AD.  These can be MEX or 3tab and should be handled appropriately.

    Output: An H5AD file should be created and the path returned.  The name of the 
           file created should use the ID passed, like this:

           f50c5432-e9ca-4bdd-9a44-9e1d624c32f5.h5ad
    
    TBD: Error with writing to file.
    """
    data_archive = DataArchive()
    dtype = data_archive.get_archive_type(data_path = dataset_dir)
    #filename = ntpath.basename(os.path.splitext(dataset_dir)[0])
    filename = str(dataset_id)
    outdir_name = os.path.normpath(output_dir + "/" + filename + ".h5ad")
    h5AD = None
    if dtype == "3tab":
        h5AD = data_archive.read_3tab_files(data_path = dataset_dir)
    elif dtype == "mex":
        h5AD = data_archive.read_mex_files(data_path = dataset_dir)
    else:
        raise Exception("Undetermined Format: {0}".format(dtype))
    if h5AD != None:
        h5AD.write_h5ad(output_path = outdir_name, gear_identifier = dataset_id)
    return outdir_name

def ensure_ensembl_index(h5_path, organism_id):
    """
    Input: An H5AD ideally with Ensembl IDs as the index.  If instead they are gene
           symbols, this function should perform the mapping.

    Output: An updated (if necessary) H5AD file indexed on Ensembl IDs after mapping.
           Returns nothing.
    """
    add_ensembl_cmd = "python3 $HOME/git/gEAR/bin/add_ensembl_id_to_h5ad_missing_release.py -i {0} -o {0}_new.h5ad -org {1}".format(h5_path, organism_id)
    run_command(add_ensembl_cmd)
    shutil.move("{0}_new.h5ad".format(h5_path), h5_path)

def extract_dataset(input_file_path, output_base):
    """
    Input: A path to an input dataset tarball, and the base directory where output can be
           written temporarily.

    Output: This function will extract the .tar or .tar.gz file and return the path to the
           directory created.

    Assumptions:  The specification states that the tar or tarball should create a unique
           directory name within which all the files of the dataset are contained.

    Example:
           Input:  /path/to/DLPFCcon322polyAgeneLIBD.3tab.tar.gz 
           Output: /path/to/DLPFCcon322polyAgeneLIBD/DLPFCcon322polyAgeneLIBD_COLmeta.tab 
                                                   ./DLPFCcon322polyAgeneLIBD_DataMTX.tab 
                                                   ./DLPFCcon322polyAgeneLIBD_ROWmeta.tab 
                                                   ./DLPFCcon322polyAgeneLIBD_EXPmeta.json
           Returns: /path/to/DLPFCcon322polyAgeneLIBD
    """
    log('INFO', "Extracting dataset at path: {0}".format(input_file_path))
    
    tar = tarfile.open(input_file_path)
    tar.extractall(path = output_base)
    tar.close()
    tar_name = ntpath.basename(input_file_path).split('.',1)[0]
    tar_path = os.path.normpath(output_base+"/"+tar_name)
    if not os.path.isdir(tar_path):
        raise Exception("Path returned was incorrect or extraction failed: {0}".format(input_file_path))
    return tar_path

def get_gear_organism_id(sample_attributes):
    data_organism_id = {'id' : [1, 2, 3, 5],
                        'label' : ['Mouse', 'Human', 'Zebrafish', 'Chicken'],
                        'taxon_id' : [10090, 9606, 7955, 9031]
                        }
    organism_id = False
    if "Human" in sample_attributes or "Homo sapiens" in sample_attributes or "9606" in sample_attributes:
        organism_id = 2 
    if "Mouse" in sample_attributes or "Mus musculus" in sample_attributes or "10090" in sample_attributes:
        organism_id = 1
    if "Zebrafish" in sample_attributes or "Danio rerio" in sample_attributes or "7955" in sample_attributes:
        organism_id = 3
    if "Chicken" in sample_attributes or "Gallus gallus" in sample_attributes or "9031" in sample_attributes:
        organism_id = 5
    return(organism_id)

def get_datasets_to_process(base_dir, output_base):
    """
    Input: A base directory with log files to process. 

    Output: A list of dataset archive files to process, like this:

        /path/to/somefilename.tab.counts.tar
        /path/to/otherfilename.tab.counts.tar

         Where the contents of these match the specification in docs/input_file_format_standard.md
    """
    formats = ['mex','MEX', 'TABCOUNTS', 'TABanalysis']
    log_file_list = os.listdir(base_dir)
    log_file_list = prepend(log_file_list, base_dir)
    paths_to_return = []
    for logfile in log_file_list:
        fname = os.path.splitext(ntpath.basename(logfile))[0]
        output = os.path.normpath(output_base + "/" + fname + ".new")
        read_log_file = pandas.read_csv(logfile, sep="\t", header=0)
        hold_relevant_entries = read_log_file.loc[read_log_file['Type'].isin(formats)]
        for entry in hold_relevant_entries.index:
            tar_path = hold_relevant_entries['Output Dir'][entry] + "/" + hold_relevant_entries['Output file'][entry]
            paths_to_return.append(tar_path)
    return paths_to_return

def get_metadata_file(base_dir):
    """
    Input: A base directory, presumably the extracted tarball of a dataset.

    Output: The full path to the file which appears to be the metadata file,
           whether that's an xls or json file
    """
    log('INFO', "Extracting metadata file from base: {0}".format(base_dir))
    file_list = os.listdir(base_dir) 
    metadata_f = False
    
    for filename in file_list:
        if "EXPmeta" in filename:
            metadata_f = os.path.normpath(base_dir+"/"+filename)

    log('INFO', "Got metadata file: {0}".format(metadata_f))
    return metadata_f

def get_organism_id(metadata_path):
    with open(metadata_path) as json_file:
        jdata = json.load(json_file)
        hold_taxid = jdata['sample_taxid']
        hold_organism = jdata['sample_organism']
    return hold_taxid, hold_organism

def log(level, msg):
    print("{0}: {1}".format(level, msg),  flush=True)

def prepend(list, str):
    """
    Input: List of files in the input directory and path to input directory

    Output: List of full paths to log files in input directory
    """
    str += '{0}'
    list = [str.format(i) for i in list] 
    return(list)

def run_command(cmd):
    log("INFO", "Running command: {0}".format(cmd))
    return_code = subprocess.call(cmd, shell=True)
    if return_code != 0:
       raise Exception("ERROR: [{2}] Return code {0} when running the following command: {1}".format(return_code, cmd, datetime.datetime.now()))

def upload_to_cloud(bucket, h5_path, metadata_json_path):
    """
    Input: Paths to both H5 and metadata files to be uploaded to a gEAR cloud instance

    Output: Files uploaded to GCloud.  No values returned.

    Further docs: 
      https://cloud.google.com/python/getting-started/using-cloud-storage
    """
    log('INFO', 'Uploading these files to the cloud bucket: {0}, {1}'.format(h5_path, metadata_json_path))
    for filename in [h5_path, metadata_json_path]:
        blob = bucket.blob(os.path.basename(filename))
        blob.upload_from_filename(filename)

if __name__ == '__main__':
    main()







