By default the files for a sample will be stored as a tarball with the following files.  The tarball itself should be called:  IDxxxx.tar.gz

**Core:**

## "Counts" files

IDxxxx_DataMTX.tab - They primary data matrix. Row keys should be Ensembl IDs where possible.  Otherwise they are keyed on gene symbol and will be mapped to Ensembl IDs during H5AD conversion.

IDxxxx_COLmeta.tab - Metadata for each column from the primary matrix.

IDxxxx_ROWmeta.tab - Metadata for each row of the primary matrix.  'gene_symbol' is a required field if Ensembl IDs

IDxxxx_EXPmeta.json - Experimental-level metadata as defined in [this example](https://github.com/jorvis/gEAR/blob/master/www/user_templates/metadata_template.xlsx).

The bundle of these 4 files will be in the format of ```<file_prefix>.tab.counts.tar```

The extraction of the tarball with tab format will look like this:

$ tar -xzvf DLPFCcon322polyAgeneLIBD.3tab.tar.gz
./DLPFCcon322polyAgeneLIBD/DLPFCcon322polyAgeneLIBD_COLmeta.tab
./DLPFCcon322polyAgeneLIBD/DLPFCcon322polyAgeneLIBD_DataMTX.tab
./DLPFCcon322polyAgeneLIBD/DLPFCcon322polyAgeneLIBD_ROWmeta.tab
./DLPFCcon322polyAgeneLIBD/DLPFCcon322polyAgeneLIBD_EXPmeta.json

Extraction of one with MEX is:

$ tar -xzvf DLPFCcon322polyAgeneLIBD.mex.tar.gz
./DLPFCcon322polyAgeneLIBD/DLPFCcon322polyAgeneLIBD_barcodes.tsv
./DLPFCcon322polyAgeneLIBD/DLPFCcon322polyAgeneLIBD_matrix.mtx
./DLPFCcon322polyAgeneLIBD/DLPFCcon322polyAgeneLIBD_genes.tsv
./DLPFCcon322polyAgeneLIBD/DLPFCcon322polyAgeneLIBD_EXPmeta.json

## "Analysis" files

**Supplemental analysis:**

IDxxxx_COLmeta_DIMRED_PCA.tab - Description needed
IDxxxx_ROWmeta_DIMRED_PCA.tab - Description needed
IDxxxx_DIMREDmeta_PCA.tab - Description needed

The bundle of these 3 files will be in the format of ```<file_prefix>-<analysis>.tab.analysis.tar```.  In the above examples, IDxxxx would be the \<file_prefix\> and PCA would be the \<analysis\>.

The extraction of the tarball with tab format will look like this:
