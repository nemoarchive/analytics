## About

This file aims to document the process flow from initial data placement in incoming to the final
files existing in the DMZ and H5AD files pushed to the gEAR instance.

## Overall process

- Data are placed into the 'incoming' directory
- Shaun's cron script processes incoming (Tuesday morning at 12:05AM)
  - Creates a diff file
  - Makes tarballs of the directory files
  - Copies tarballs to the DMZ

## Incoming

The 'incoming' directory tree root is:

    /local/projects-t3/NEMO/incoming

## DMZ

The 'DMZ' directory tree root is:

    /local/projects-t3/NEMO/dmz

## gEAR instance

