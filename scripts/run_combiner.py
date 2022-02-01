import hail as hl
import argparse
import os
import logging

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
		"temp-dir",
		metavar="temp_dir",
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
	
	# must provide a sample_map
	path_to_sample_map = os.path.abspath(parser.sample_map)

    # no provided output path or temp_dir, defaults to cwd
	path_to_output = os.path.abspath(parser.output)	
	#path_to_temp_dir = os.path.abspath(parser.temp_dir)
	
#	if hasattr(parser, "on_hdfs"):
#		sample_prepend = "hdfs://"
#	else:
#		sample_prepend = "file://"
		
	if parser.app_name:
		app_name = parser.app_name
	else:
		app_name = ""
		
	hl.init(
		log=path_to_output
	)	
	
	inputs = []
	with hl.hadoop_open(path_to_sample_map, "r") as f:
		for line in f:
			inputs.append(line.strip())
			
	hl.experimental.run_combiner(
		inputs, 
		out_file=path_to_output, 
		tmp_path=path_to_output,
		reference_genome=parser.reference,
		use_genome_default_intervals=True,
		overwrite=True
	)

	

if __name__ == "__main__":
	main()
