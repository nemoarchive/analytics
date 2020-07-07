## About

This file aims to document the process flow from initial data placement in incoming to the final
files existing in the DMZ and H5AD files pushed to the gEAR instance.

## Overall process

- Data are placed into the 'incoming' directory
- Shaun's cron script processes incoming (Tuesday morning at 12:05AM)
  - Creates diff files for dmz and incoming.  gEAR will process the dmz one.
  - Makes tarballs of the directory files
  - Copies tarballs to the DMZ

NOTE: This process will change a bit when Shaun rolls out the updated ingesting scripts.

## Incoming

The 'incoming' directory tree root is:

    /local/projects-t3/NEMO/public/incoming

The diff file for incoming is generated Tuesday morning at 12:05 AM.  The diff filename will be in the format of ```<project>-YYYY-MM-DD.diff``` where \<project\> is either "BICCN", "BICCN", or "other".

The script /local/projects-t3/NEMO/bin/nemo_bin/preprocess_nemo_bundles.py is run as user "nemo_dmz" on the "incoming" .diff file:

1. Perform QC checks
    1. Ensure files exist, have size, and do not have a .aspx file.  Some Aspera transfers may still be ongoing or were incomplete
    2. Ensure files that go in a bundle have the other component files as well.
    3. Files that do not pass QC get written to an errors file that explains error briefly
2. NeMO Identifiers are generated for the potential bundle and for the component files
    - Files to be copied instead of bundled will not have component identifiers assigned
3. Move the component files from "incoming" area to "processed" area
    - If file is unable to be moved due to permissions, the bundle will not be written to output
    - If file already exists in the "processed" area, then we can assume this is a newer version.
      - File will change from <file_prefix>.<ext> to <file_prefix>.v#.<ext>
      - All identifiers with this file_prefix/file_type combination will reference the latest file via identifier
4. Write bundle information into output, and insert component identifiers into mysql db
    - Bundle identifiers will be written after successful bundling or copying

### Tab-delimited output of preprocess_nemo_files.py

1. "tar" or "cp" (to indicate if how to move the file from "processed to "dmz")
2. File prefix
3. Version of file
4. Comma-separated list of all component files in bundle.  For "cp" it is just one file and ignored
5. File type
6. Identifier for file bundle
7. Output extension to use after files are bundled
8. Directory of "processed" files.  Generally this will mirror the "incoming" directory but there are exceptions (CEMBA files, for example)
9. Directory for "dmz" files.  This will mirror the "processed" files directory

## Processed

The 'processed' directory tree root is:

    /local/projects-t3/NEMO/public/processed

The output file in preprocess_nemo_files.py is passed into bundle_nemo_files.pl, which does the following:

1. Copies files with "cp" in the first field to "dmz"
2. Bundles files with "tar" in the first field to "dmz" by farming commands as grid jobs
3. After file is moved, file identifier is inserted into mysql database
    - Versioning of files and adjusting identifiers still applies

### Tab-delimited output of bundle_nemo_files.pl

1. File type
2. File prefix
3. File version
4. Processed directory of original or component files
5. DMZ directory of bundle
6. Bundled file basename
7. Grid ID of job.  "NA" for copied jobs
8. Job status code based on exit status from grid.  "C" for complete. "F" for failure. "NA" for copied jobs
9. MD5 of bundled file
10. Size (in bytes) of bundled file
11. MTime of bundled file
12. Identifier of bundled file

## DMZ

The 'DMZ' directory tree root is:

    /local/projects-t3/NEMO/public/release (symlinked to /local/projects-t3/NEMO/dmz)

The diff file for incoming is generated Thursday morning at 12:05 AM. The diff filename will be in the format of ```dmz-<project>-YYYY-MM-DD.diff``` where \<project\> is either "BICCC", "BICCN", or "other".

Only data in the dmz will be ingested for gEAR.

Question - are we doing all labs, or only a subset (biccc, biccn, etc?)

## gEAR instance

