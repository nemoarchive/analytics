#!/usr/bin/env python3

"""

This is a script to read a log directory of expression data ready to parse, perform
validation, convert to H5AD, then push to the cloud node for import by a gEAR instance.

"""

import argparse
import os
import uuid
from gear.datasetuploader import FileType, DatasetUploader
import gear.mexuploader
from gear.metadatauploader import MetadataUploader
import pandas
import tarfile
import json
import ntpath
import csv
import itertools

def main():
    parser = argparse.ArgumentParser( description='NeMO data processor for gEAR')

    parser.add_argument('-ilb', '--input_log_base', type=str, required=True, help='Path to the base directory where the logs are found' )
    parser.add_argument('-ob', '--output_base', type=str, required=True, help='Path to a local output directory where files can be written while processing' )
    args = parser.parse_args()

    files_pending = get_datasets_to_process(args.input_log_base, args.output_base)
    for file_path in files_pending:
        dataset_id = uuid.uuid4()
        dataset_dir = extract_dataset(file_path, args.output_base)
        metadata_file_path = get_metadata_file(dataset_dir)
        metadata_is_valid  = validate_metadata_file(metadata_file_path)
        
        if metadata_is_valid:
            metadata_json_path = create_metadata_json(metadata_file_path, dataset_id)
            h5_path = convert_to_h5ad(dataset_dir, dataset_id)
            
            ensure_ensembl_index(h5_path)

            upload_to_cloud(h5_path, metadata_json_path)

            
def convert_to_h5ad(dataset_dir, dataset_id):
    """
    Input: An extracted directory containing expression data files to be converted
           to H5AD.  These can be MEX or 3tab and should be handled appropriately.

    Output: An H5AD file should be created and the path returned.  The name of the 
           file created should use the ID passed, like this:

           f50c5432-e9ca-4bdd-9a44-9e1d624c32f5.h5ad
    """
    return ""

def create_metadata_json(input_file_path, dataset_id):
    """
    Input: A metadata file in XLS or JSON format, validated.
 
    Output: The metadata file converted to JSON and named using the dataset ID, 
           returning the full path of the resulting JSON file.
    """
    return ""

def ensure_ensembl_index(h5_path):
    """
    Input: An H5AD ideally with Ensembl IDs as the index.  If instead they are gene
           symbols, this function should perform the mapping.

    Output: An updated (if necessary) H5AD file indexed on Ensembl IDs after mapping.
           Returns nothing.
    """
    pass

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
    tar = tarfile.open(input_file_path)
    tar.extractall(path= output_base)
    tar.close()
    tar_name = ntpath.basename(input_file_path).split('.',1)[0]
    tar_path = os.path.normpath(output_base+"/"+tar_name)
    if not os.path.isdir(tar_path):
        raise Exception("Path returned was incorrect or extraction failed: {0}".format(input_file_path))
    return tar_path

def get_datasets_to_process(base_dir, output_base):
    """
    Input: A base directory with log files to process. 

    Output: A list of dataset archive files to process, like this:

        /path/to/somefilename.tab.counts.tar
        /path/to/otherfilename.tab.counts.tar

         Where the contents of these match the specification in docs/input_file_format_standard.md
    """
    formats = ['mex','MEX', 'TABcounts', 'TABanalysis']
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
    file_list = os.listdir(base_dir) 
    metadata_f = False
    for filename in file_list:
        if "_EXPmeta" in filename:
            metadata_f = os.path.normpath(base_dir+"/"+filename)
    return metadata_f

def prepend(list, str):
    """
    Input: List of files in the input directory and path to input directory

    Output: List of full paths to log files in input directory
    """
    str += '{0}'
    list = [str.format(i) for i in list] 
    return(list)

def validate_metadata_file(file_path):
    """
    Input: A metadata file path in XLS or JSON format.
    
    Output: Returns True/False reflecting the validation of the metadata file. This should
           include checking required parameters and general formatting of the file.
    TBD: Need a validation class for metadata. 
    """
    if not os.path.isfile(file_path):
        raise Exception("Path returned was incorrect: {0}".format(file_path))

    md = MetaData(file_path=file_path)
    return md.validate()


def upload_to_cloud(h5_path, metadata_json_path):
    """
    Input: Paths to both H5 and metadata files to be uploaded to a gEAR cloud instance

    Output: Files uploaded to GCloud.  No values returned.
    """
    # These could be made configurable
    gcloud_project = 'nemo-analytics'
    gcloud_instance = 'nemo-prod-201904'

if __name__ == '__main__':
    main()







