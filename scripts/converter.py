#--diffpath (path to diff files with only "NEW"
#--newdir (path to file with directories from ^)
#--outdir (path to output directory)
#--rootdir (path to root dir to append to paths pulled from diff file)


import argparse
import ntpath
import os, sys
from optparse import OptionParser
import getopt
from gear.datasetuploader import FileType, DatasetUploader
import gear.mexuploader
from gear.metadatauploader import MetadataUploader
import pandas
import tarfile
import json

parser = OptionParser()
parser.add_option("-d", "--diffpath", dest="diffpath",help="Path to new diff files", metavar="FILE")
parser.add_option("-n", "--newpath", dest="newdir",help="Path to file with new files", metavar="FILE")
parser.add_option("-o", "--outdir", dest="outdir",help="Path to Output Directory", metavar="PATH")
parser.add_option("-r", "--rootdir", dest="rootdir",help="Path to root directory",default = "/local/projects-t3/NEMO/dmz/", metavar="PATH")

(options, args) = parser.parse_args()

def new_direc(filepath):
	with open(filepath, "r") as new_dir:
		for direc in new_dir:
			filep=options.rootdir + direc.strip()
			dir_list=os.listdir(filep)
			#out_d= options.outdir + ntpath.basename(os.path.splitext(direc)[0])
			for entry in dir_list:
				file_type=entry.rsplit('.', 1)[1]
				h5_out_path=options.outdir + ntpath.basename(os.path.splitext(entry)[0])+'.h5ad'
				pathf=filep + entry
				if file_type == "tar" or file_type=="gz":
					dsu=DatasetUploader()
					dataset_uploader=dsu.get_by_filetype(filetype=file_type, filepath=pathf)
					try:
						dataset_uploader._read_file(pathf)
						rows_X,cols_X=dataset_uploader.adata.X.shape
						dataset_uploader.adata.write(h5_out_path)
						print(rows_X, cols_X)
					except Exception as err:
						print(str(err))
					hold3=metadataV(pathf)	


def metadataV(tarp):
	in_tar=tarfile.open(tarp, 'r')
	t_filenames = in_tar.getnames()
	for i in t_filenames:
		if "_EXPmeta" in i:
			metfile=i
			mdu=MetadataUploader()
			filetype_MD = metfile.rsplit('.', 1)[1]
			out_n= ntpath.basename(os.path.splitext(metfile)[0])
			if(filetype_MD == "xlsx" or filetype_MD == "xls"):
				json_out=options.outdir + out_n + '.json'
				metadata = mdu._read_file(metfile)
				mdu._write_to_json(filepath= json_out)
				attr_hold=get_attributes(json_out)
				print(attr_hold)
			elif(filetype_MD == "json"):
				print(in_tar.getmember(i))
				in_tar.extract(i, path = options.outdir)
				json_p = options.outdir + i
				attr_hold=get_attributes(json_p)
				print(attr_hold)


def get_attributes(jsonf):
	with open(jsonf) as json_file:
		jdata = json.load(json_file)
		hold_taxid= jdata['sample_taxid']
		hold_release=jdata['annotation_release_number']
	return hold_taxid, hold_release


def main():
	if options.diffpath:
		with open(options.diffpath, "r") as f:
			for line in f:
				clean=line.strip()
				if clean:
					read_list=clean
					fname=ntpath.basename(os.path.splitext(read_list)[0])
					print(fname)
					syscmd="grep \"NEW\" " + read_list + " > " + options.outdir + "/" + fname +"_new_extracted.diff"
					os.system(syscmd)
					syscmd=" awk '{print $5}' "+ options.outdir + fname + "_new_extracted.diff"+ " > " + options.outdir+ "/" + fname + "_new.dirs"
					os.system(syscmd)
					hold1=new_direc(options.outdir+ "/" + fname + "_new.dirs")
					print(clean)
					print(options.rootdir)



	if options.newdir:
		#print("Newdir given")
		hold2=new_direc(options.newdir)


if __name__ == '__main__':
    main()
