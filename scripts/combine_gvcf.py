#---------------------------------------------------#
#                 Joint-call GVCFs                  #
#---------------------------------------------------#

import argparse
import os
import logging
from datetime import datetime
import hail as hl

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
		help="sample_name_map, output and temp directories are stored on the hdfs"
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

	# Initialise logger
	init_log(os.path.abspath(parser.output))

	parser = make_argparser()
	
	# Set name for the Hail application
	if parser.app_name:
		name = parser.app_name
	else:
		name = "combine_gvcf"

	# Prepend location according for files. Default is local.
	if hasattr(parser, "on_hdfs"):
		prepend_location = "hdfs://"
	else:
		prepend_location = "file://"

	# Path to the sample map
	path_to_sample_map = prepend_location + os.path.abspath(parser.sample_map)

	# Path to output destination
	path_to_output = prepend_location + os.path.abspath(parser.output)

    # Path to location for storing intermediate files
	path_to_temp_dir = prepend_location + os.path.abspath(parser.temp_dir)

	# Set choice for possibly overwriting output
	if hasattr(parser, "overwrite"):
		overwrite_choice = True
	else: 
		overwrite_choice = False	

	logging.info(f"Initialising Hail")
	hl.init(
		app_name = name,
		log=os.getcwd()		# This log location must be local
	)	
	
	# Generate list of input files
	inputs = []
	with hl.hadoop_open(path_to_sample_map, "r") as f:
		for line in f:
			inputs.append(line.strip())

	logging.info(f"Combining {len(inputs)} GVCFs...")

	# Run Hail's combiner
	hl.experimental.run_combiner(
		inputs, 
		out_file=path_to_output, 
		tmp_path=path_to_temp_dir,
		reference_genome=parser.reference,
		use_genome_default_intervals=True,	
		key_by_locus_and_alleles=True,		# This ensures the alleles row field isn't removed downstream
		overwrite=overwrite_choice
	)

	logging.info(f"Wrote combined GVCFs out to {path_to_output}")

def init_log(path_to_output):
    now = datetime.now()
    dt_string = now.strftime("%d_%m_%Y_%I_%M_%S_%p")
    log_location = os.path.join(os.path.dirname(path_to_output), f"combine_gvcf_{dt_string}.log")

    logging.basicConfig(
        filename=log_location, 
        format="%(asctime)s %(message)s", 
        datefmt="%d/%m/%Y %I:%M:%S %p - ",
        level=logging.INFO
    )

if __name__ == "__main__":
	main()
