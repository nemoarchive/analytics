## About

This file aims to document the process flow from initial data placement in incoming to the final
files existing in the DMZ and H5AD files pushed to the gEAR instance.

## Overall process

- Data are placed into the 'incoming' directory
- Shaun's cron script processes incoming (Tuesday morning at 12:05AM)
  - Creates diff files for dmz and incoming.  gEAR will process the dmz one.
  - Makes tarballs of the directory files
  - Copies tarballs to the DMZ

## Incoming

The 'incoming' directory tree root is:

    /local/projects-t3/NEMO/incoming

The diff file for incoming is generated Tuesday morning at 12:05 AM.  The diff filename will be in the format of "<project>-YYYY-MM-DD.diff" where <project> either "BICCN", "BICCN", or "other".

## DMZ

The 'DMZ' directory tree root is:

    /local/projects-t3/NEMO/dmz

The diff file for incoming is generated Thursday morning at 12:05 AM. The diff filename will be in the format of "dmz-<project>-YYYY-MM-DD.diff" where <project> either "BICCN", "BICCN", or "other".

Only data in the dmz will be ingested for gEAR.

Question - are we doing all labs, or only a subset (bicc, biccn, etc?)

## gEAR instance

