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

import argparse, json, os, sys
import datetime
import uuid
import pandas
import tarfile
import ntpath
import csv
import itertools
import subprocess
import shutil
import logging

# Read from config file
import configparser
conf_loc = os.path.join(os.path.dirname(__file__), '.conf.ini')
if not os.path.isfile(conf_loc):
    sys.exit("Config file could not be found at {}".format(conf_loc))
config = configparser.ConfigParser()
config.read(conf_loc)

# Necessary if we are to read from db
sys.path.append(config.get("paths", "ident_api_path"))
import sqlalchemy as db
import mysql_tables as tables

from google.cloud import storage
GCLOUD_PROJECT = config.get("gcloud", "project")
GCLOUD_BUCKET = config.get("gcloud", "bucket")

#Path to single log file with processed datasets
PROCESSED_LOGFILE = config.get("paths", "cron_upload_log")

# Read gEAR library modules
sys.path.append(os.path.join(config.get("paths", "gear_dir"), "lib"))
from gear.metadata import Metadata
from gear.dataarchive import DataArchive

def main():
    parser = argparse.ArgumentParser( description='NeMO data processor for gEAR')

    inputtype = parser.add_mutually_exclusive_group(required=True)
    #inputtype.add_argument('-ilb', '--input_log_base', type=str, help='Path to the base directory where the logs are found' )
    inputtype.add_argument('-id', '--input_directory', type=str, help='Path to a single input directory with tar files' )
    inputtype.add_argument('-l', '--list_file', help='Path to a file containing a list of bundled tar files.')
    inputtype.add_argument('-m', '--manifest_file', help='Path to a file manifest containing `ls -l` contents')
    inputtype.add_argument('-db', '--database', help="Get files out of database.  Credentials provide by config file", action='store_true')
    parser.add_argument('-ob', '--output_base', type=str, required=True, help='Path to a local output directory where files can be written while processing' )
    parser.add_argument('-s', '--metadata_xls', required=True, help='Path to a Excel-formatted spreadsheet of metadata')
    args = parser.parse_args()

    # TODO: Research OAuth2 service accounts and see if that is a better method than exporting credentials on command line
    sclient = storage.Client(project=GCLOUD_PROJECT)
    bucket = storage.bucket.Bucket(client=sclient, name=GCLOUD_BUCKET)

    # TODO: Eventually add from identifiers database
    if args.input_directory:
        log("INFO", "Reading from input directory")
        files_pending = get_tar_paths_from_dir(args.input_directory)
    elif args.list_file:
        log("INFO", "Reading from a list file")
        with open(args.list_file) as f:
            files_pending = [line.rstrip() for line in f]
    elif args.manifest_file:
        log("INFO", "Reading from inventory manifest file")
        with open(args.manifest_file) as f:
            all_lines = [line.rstrip() for line in f]
            files_pending = get_tar_paths_from_manifest(all_lines)
    else:
        log("INFO", "Reading from database")
        files_pending = get_tar_paths_from_database()
        # Phasing out reading from logfile
        #files_pending = get_datasets_to_process(args.input_log_base, args.output_base, PROCESSED_LOGFILE)

    for file_path in files_pending:
        log('INFO', "Processing datafile at path:{0}".format(file_path))
        dataset_id = uuid.uuid4()
        dataset_dir = extract_dataset(file_path, args.output_base)
        metadata_file_path = get_metadata_file(dataset_dir, file_path, args.metadata_xls)
        metadata = Metadata(file_path=metadata_file_path)
        logger = setup_logger()
        if metadata.validate():
            log('INFO', "Metadata file is valid: {0}".format(metadata_file_path))
            try:
                metadata_json_path = "{0}/{1}.json".format(args.output_base, dataset_id)
                metadata.write_json(file_path=metadata_json_path)
                organism_taxa = get_organism_id(metadata_file_path)
                # Ensure organism_taxa is string in case Int is passed through JSON
                organism_id = get_gear_organism_id(str(organism_taxa))
                if organism_id == -1:
                    raise
                h5_path, is_en = convert_to_h5ad(dataset_dir, dataset_id, args.output_base)
                ensure_ensembl_index(h5_path, organism_id, is_en)
                logger.info(file_path, extra={"dataset_id":dataset_id, "status": h5_path})
                upload_to_cloud(bucket, h5_path, metadata_json_path)
            except:
                log('ERROR', "Failed to process file:{0}".format(file_path))
                logger.info(file_path, extra={"dataset_id":dataset_id, "status":"FAILED"})
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
    is_en = False
    data_archive = DataArchive()
    dtype = data_archive.get_archive_type(data_path = dataset_dir)
    #filename = ntpath.basename(os.path.splitext(dataset_dir)[0])
    filename = str(dataset_id)
    outdir_name = os.path.normpath(output_dir + "/" + filename + ".h5ad")
    h5AD = None
    if dtype == "3tab":
        h5AD, is_en = data_archive.read_3tab_files(data_path = dataset_dir)
    elif dtype == "mex":
        h5AD, is_en = data_archive.read_mex_files(data_path = dataset_dir)

    else:
        raise Exception("Undetermined Format: {0}".format(dtype))
    if h5AD != None:
        h5AD.write_h5ad(output_path = outdir_name, gear_identifier = dataset_id)
    return outdir_name, is_en

def ensure_ensembl_index(h5_path, organism_id, is_en):
    """
    Input: An H5AD ideally with Ensembl IDs as the index.  If instead they are gene
           symbols, this function should perform the mapping.

    Output: An updated (if necessary) H5AD file indexed on Ensembl IDs after mapping.
           Returns nothing.
    """

    gear_bin_dir = os.path.join(config.get("paths", "gear_dir"), "bin")
    if is_en == False:
        add_ensembl_cmd = "python3 {0}/add_ensembl_id_to_h5ad_missing_release.py -i {1} -o {1}_new.h5ad -org {2}".format(gear_bin_dir, h5_path, organism_id)
        run_command(add_ensembl_cmd)
        shutil.move("{0}_new.h5ad".format(h5_path), h5_path)
    else:
        add_ensembl_cmd = "/usr/local/common/Python-3.7.2/bin/python3 {0}/find_best_ensembl_release_from_h5ad.py -i {1} -org {2}".format(gear_bin_dir, h5_path, organism_id)
        run_command(add_ensembl_cmd)


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
    """
    data_organism_id = {'id' : [1, 2, 3, 5],
                        'label' : ['Mouse', 'Human', 'Zebrafish', 'Chicken'],
                        'taxon_id' : [10090, 9606, 7955, 9031]
                        }
    """
    if sample_attributes.lower() in ["human", "homo sapiens","9606"]:
        return 2
    if sample_attributes.lower() in ["mouse","mus musculus", "10090"]:
        return 1
    if sample_attributes.lower() in ["zebrafish", "danio rerio", "7955"]:
        return 3
    if sample_attributes.lower() in ["chicken", "gallus gallus", "9031"]:
        return 5
    log("ERROR", "Could not associate organism or taxon id {} with a gEAR organism ID".format(sample_attributes))
    return -1

def get_datasets_to_process(base_dir, output_base, processed_log):
    """
    Input: A base directory with log files to process.

    Output: A list of dataset archive files to process, like this:

        /path/to/somefilename.tab.counts.tar
        /path/to/otherfilename.tab.counts.tar

         Where the contents of these match the specification in docs/input_file_format_standard.md
    """

    # Open file of previously processed files for reading
    processed_ds = pandas.read_csv(processed_log, sep='\t', usecols = ['Processed_Files'], header=0)
    formats = [i.upper() for i in ['MEX', 'TABanalysis', 'TABcounts']]

    # Gather all of the bundle log output files
    log_file_list = list()
    for entry in os.listdir(base_dir):
        if entry.endswith('diff.log'):
            log_file_list.append(entry)

    log_file_list = prepend(log_file_list, base_dir)
    paths_to_return = []
    # Open each logfile and get all MEX, TABanalysis, and TABcounts files
    for logfile in log_file_list:
        log('INFO', "Processing log file: {0}".format(logfile))
        read_log_file = pandas.read_csv(logfile, sep="\t", header=0)
        hold_relevant_entries = read_log_file.loc[read_log_file['Type'].upper().isin(formats)]
        for entry in hold_relevant_entries.index:
            tar_path = os.path.normpath(hold_relevant_entries['Output Dir'][entry] + "/" + hold_relevant_entries['Output file'][entry])
            if not processed_ds['Processed_Files'].str.contains(tar_path).any():
                paths_to_return.append(tar_path)
    return paths_to_return

def get_metadata_file(base_dir, dmz_path, metadata_sheet):
    """
    Input: A base directory, presumably the extracted tarball of a dataset and the path to archive being processed.

    Output: The full path to the file which appears to be the metadata file,
           whether that's an xls or json file
    """
    log('INFO', "Extracting metadata file from base: {0}".format(base_dir))
    file_list = os.listdir(base_dir)
    metadata_f = False
    dtype = DataArchive()
    dtype = dtype.get_archive_type(data_path = base_dir)
    if dtype == "3tab":
        for filename in file_list:
            if "EXPmeta" in filename:
                metadata_f = os.path.normpath(base_dir+"/"+filename)
    elif dtype == "mex":
        metadata_fetch = "{}/get_sample_by_file/nemo_get_metadata_for_file.py -s {} ".format(config.get("paths", "nemo_scripts_bin"), metadata_sheet)
        output_path = os.path.normpath(base_dir + "/" + "EXPmeta_generated.json")
        metadata_cmd ="python3 "+ metadata_fetch + " -i "+ dmz_path + " -o " + output_path
        #not using subroutine as we might need to change how we run the command once the script is finalized by Shaun
        metadata_cmd = subprocess.call(metadata_cmd, shell = True)
        metadata_f = output_path
    log('INFO', "Got metadata file: {0}".format(metadata_f))
    return metadata_f

def get_organism_id(metadata_path):
    with open(metadata_path) as json_file:
        jdata = json.load(json_file)
        if 'sample_taxid' in jdata:
            return jdata['sample_taxid']
        elif 'taxon_id' in jdata:
            return jdata['taxon_id']
        raise Exception("No taxon id provided in file {}".format(metadata_path, datetime.datetime.now()))

def get_tar_paths_from_database():
    """Use a database to read out the files."""
    extensions = ['.mex.tar.gz', '.tab.analysis.tar', '.tab.counts.tar']

    http_ptrn = config.get("paths", "http_path")
    # Path to "release/public-facing" area
    release_dir_ptrn = os.path.join(config.get("paths", "release_dir"), "brain")

    conn = setup_mysql(config.get("mysql", "ip"), config.get("mysql", "db"), config.get("mysql", "user"), config.get("mysql", "pass"))

    # Get all "derived" identifier records
    query = db.select([tables.derived.columns.file_url])
    result_proxy = conn.execute(query)

    # Iterate through records and only keep MEX, TABcounts, and TABanalysis bundles
    desired_files = []
    for row in result_proxy:
        for e in extensions:
            if row[0].endswith(e):
                # Replace HTTP URL with "release/public-facing" pathname
                desired_files.append(row[0].replace(http_ptrn, release_dir_ptrn))
                break
    return desired_files

def get_tar_paths_from_dir(base_dir):
    tar_list = list()
    for entry in os.listdir(base_dir):
        if entry.endswith('.tar') or entry.endswith('.tar.gz'):
            tar_list.append("{0}/{1}".format(base_dir, entry))
    return tar_list

def get_tar_paths_from_manifest(lines):
    """Iterate through manifest filehandle to retrieve tar files that fit the formats desired."""
    extensions = ['.mex.tar.gz', '.tab.analysis.tar', '.tab.counts.tar']

    def is_good_file(filename):
        """Does file end with a desired file extension?"""
        for e in extensions:
            if filename.endswith(e):
                return True
        return False

    # Path to "release/public-facing" area
    release_dir = config.get("paths", "release_dir")

    # Extract files out of the `ls -l` output
    files = [line.split()[-1] for line in lines]
    # Only keep files with extensions we care about
    desired_files = filter(is_good_file, files)
    # Manifest file paths were relative to a specific directory, so add the directory back
    return [os.path.join(release_dir, f) for f in desired_files]

def log(level, msg):
    print("{0}: {1}".format(level, msg),  flush=True)

def prepend(filelist, input_dir):
    """
    Input: List of files in the input directory and path to input directory

    Output: List of full paths to log files in input directory
    """
    input_dir += '{0}'
    return [input_dir.format(i) for i in filelist]

def run_command(cmd):
    log("INFO", "Running command: {0}".format(cmd))
    return_code = subprocess.call(cmd, shell=True)
    if return_code != 0:
       raise Exception("ERROR: [{2}] Return code {0} when running the following command: {1}".format(return_code, cmd, datetime.datetime.now()))

def setup_logger():
    """Set up the logger."""
    logger = logging.getLogger('tracking_log')
    logger.setLevel(logging.INFO)
    #Where to Store needs to be identified?
    f_handler = logging.FileHandler(PROCESSED_LOGFILE, mode='a', encoding = None, delay = False)
    f_handler.setLevel(logging.INFO)
    f_format = logging.Formatter('%(asctime)s\t%(message)s\t%(dataset_id)s\t%(status)s')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)
    return logger

def setup_mysql(host, database, user, pw):
    """Connect to MySQL and return database object."""
    engine = db.create_engine('mysql+mysqldb://{}:{}@{}:3306/{}'.format(user, pw, host, database))
    tables.create_tables(engine)
    return engine.connect()

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







