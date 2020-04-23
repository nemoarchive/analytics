#!/opt/bin/python3

"""
"""

import cgi, json
import os, sys

original_stdout = sys.stdout
sys.stdout = open(os.devnull, 'w')

lib_path = os.path.abspath(os.path.join('..', '..', 'lib'))
sys.path.append(lib_path)
import geardb

import scanpy as sc
sc.settings.verbosity = 0

def main():
    form = cgi.FieldStorage()
    analysis_id = form.getvalue('analysis_id')
    analysis_type = form.getvalue('analysis_type')
    dataset_id = form.getvalue('dataset_id')
    session_id = form.getvalue('session_id')
    user = geardb.get_user_from_session_id(session_id)

    ana = geardb.Analysis(id=analysis_id, type=analysis_type, dataset_id=dataset_id, 
                          session_id=session_id, user_id=user.id)

    adata = ana.get_adata()

    sc.tl.pca(adata, zero_center=True) 

    adata.uns['DimReduction'] = adata.obsm.X_pca  ## this is cells X PC 

    adata.uns['DimReductionGene'] = adata.varm['PCs'] ## this is genes X PC 

    dest_datafile_path = None #? 
    
    # primary or public analyses won't be after this
    if ana.type == 'primary' or ana.type == 'public':
        ana.type = 'user_unsaved'

    dest_datafile_path = ana.dataset_path()
    dest_directory = os.path.dirname(dest_datafile_path)

    if not os.path.exists(dest_directory):
        os.makedirs(dest_directory)

    if save_dataset:

        adata.write(dest_datafile_path)

    sys.stdout = original_stdout
    print('Content-Type: application/json\n\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
