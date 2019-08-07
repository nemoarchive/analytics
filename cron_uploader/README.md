# Overall process
Objective: This will be a python3-based script, executed via cron, which uses the gEAR API to process data files deposited in the NeMO incoming directory hierarchy, convert them to H5AD, determine EN release number, and migrate necessary files to the NeMO-analytics instance of gEAR in Google Cloud.

**Part 1: Obtaining list of new paths**

* Shaun suggested we use either the log or info files present here: /local/projects-t3/NEMO/incoming/brain/bundle_jobs. For our purpose log file should work. Info file has a couple extra columns (md5, size, mtime). 
* The log file has the following information : Type,	Prefix,	Input Dir, Output Dir, Output file and Grid ID Exit Status. We can use Type to pullonly mex and 3tab.
* Possible "Type" values: BAM, FASTQ, TSV, BED, BIGBED, BIGWIG, MEX, TABcounts, TABanalysis, CSV, FPKM
* The bundling script runs every Wednesday, and the date on the file reflects the date of the .diff file created in incoming.  The first line is a header line that has information useful for you (incoming directory, prefix, etc)
* various file patterns that Shaun uses for each filetype, check /local/devel/sadkins/nemo_bin/validate_nemo_files.py
* cron job will live on cronmaster on **tartarus**

**Part 2: Converter**

* Go through each new path and read in the tarball : **Supported**. TBD if cron output will change
* Determine format MEX/3Tab : **Supported**. Using dsu.get_by_filetype (Have added support for tab file to DSU)
* Generate h5AD files : **Supported for Essential files**. Conversion for analysis is **NOT** supported yet.
* Read Metadata file and determine format : **Supported**(xlxs, xls or json)
* Validate Metadata : Not sure what this involves

**Part 3: Processor**

* Pull Metadata attributes (sample_taxid and annotation_release_number) for script to determine release ID database: **Supported**
* Determine release ID : **Database Issue on spiny**
* Replace Gene ID to En ID in h5AD output (add_ensembl_ids_to_h5ad.py)
* Generate Tabix for EpiViz
* Move to directory to be uploaded

**Part 2 and Part 3 are currently the same script, there were suggestions to split it up in standup**
