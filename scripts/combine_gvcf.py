import hail as hl
import argparse
import os
import logging	# Need to implement this down the line

def make_argparser():
	
	parser = argparse.ArgumentParser(
		description = "A tool for utilising Hail for joint calling"
	)
	
	parser.add_argument(
		"sample_map",
		metavar="sample_map",
		type=str,
		help="path to the file containing a list of samples"
	)	
	
	parser.add_argument(
		"output",
		metavar="output",
		nargs="?",
		default=os.getcwd(),
		type=str,
		help="path to the output file"
	)
	
	parser.add_argument(
		"temp_dir",
		metavar="temp-dir",
		nargs="?",
		default=os.getcwd(),
		type=str,
		help="path for intermediate files"
	)
	
	parser.add_argument(
		"--hdfs",
		action="store_true",
		help="sample files are stored on the hdfs"
	)

	parser.add_argument(
		"--overwrite",
		action="store_true",
		help="overwrite existing output file"
	)
	
	parser.add_argument(
		"--app-name",
		metavar="app_name",
		type=str,
		help="name of the application"
	)
	
	parser.add_argument(
		"--reference",
		metavar="reference",
		default="GRCh38",
		type=str,
		help="human genome reference to be used"
	
	)
	
	return parser.parse_args()

def main():
	parser = make_argparser()
	
	# Set name for the Hail application
	if parser.app_name:
		name = parser.app_name
	else:
		name = "Hail"

	# Set the location to prepending on location of files. Default is local.
	# Should this even be an option?
	if hasattr(parser, "on_hdfs"):
		prepend_location = "hdfs://"
	else:
		prepend_location = "file://"

	# Path to the sample map
	path_to_sample_map = "file://" + os.path.abspath(parser.sample_map)

	# Path to output destination
	path_to_output = os.path.abspath(parser.output)
	if path_to_output == os.getcwd():
		path_to_output = "file://" + os.path.join(path_to_output, "result.mt")	# Implement a timestamp into the name of the result file
	else:
		path_to_output = "file://" + path_to_output

    # Path to location for storing intermediate files
	path_to_temp_dir = "file://" + os.path.abspath(parser.temp_dir)

	if hasattr(parser, "overwrite"):
		overwrite_choice = True
	else: 
		overwrite_choice = False	

	hl.init(
		app_name = name,
		log=os.getcwd()
	)	
	
	inputs = []
	with hl.hadoop_open(path_to_sample_map, "r") as f:
		for line in f:
			inputs.append(line.strip())
			
	hl.experimental.run_combiner(
		inputs, 
		out_file=path_to_output, 
		tmp_path=path_to_temp_dir,
		reference_genome=parser.reference,
		use_genome_default_intervals=True,	# implement option to choose another interval?
		key_by_locus_and_alleles=True,		# This ensures the alleles row field isn't removed downstream
		overwrite=overwrite_choice
	)

if __name__ == "__main__":
	main()
