import argparse
import os 

def make_argparser():
	
	parser = argparse.ArgumentParser(
		description = "A tool utilising Hail for joint variant calling"
	)
	
	subparsers = parser.add_subparsers(help="Sub-command help")

	joint_call = subparsers.add_parser(
		"joint-call",
		help="perform joint call with Hail"
	)

	get_sample_name_map = subparsers.add_parser(
		"get-sample-name-map",
		help="generate file list of entire cohort"
	)

	complete_QC = subparsers.add_parser(
		"complete-QC",
		help="complete the process of variant and sample QC"
	)

	# JOINT_CALL subcommand
	joint_call.set_defaults(command="joint_call")
	joint_call.add_argument(
		"sample_name_map",
		metavar="sample_name_map",
		type=str,
		help="the file containing samples to joint-call"
	)

	joint_call.add_argument(
		"output_dir",
		metavar="output_dir",
		nargs="?",
		default=os.getcwd(),
		type=str,
		help="the directory for the final output"
	)

	joint_call.add_argument(
		"tmp_dir",
		metavar="tmp_dir",
		nargs="?",
		default=os.getcwd(),
		type=str,
		help="the directory for intermediate files"
	)

	# GET_SAMPLE_NAME_MAP subcommand
	get_sample_name_map.set_defaults(command="get_sample_name_map")
	get_sample_name_map.add_argument(
		"cohort_dir",
		metavar="cohort_dir",
		type=str,
		help="the directory containing all samples belonging to the cohort"
	)

	get_sample_name_map.add_argument(
		"--output",
		type=str,
		default="sample_name_map",
		help="the name of the output file"
	)

	get_sample_name_map.add_argument(
		"--append",
		action="store_true",
		help="append cohort to existing file"
	)

	get_sample_name_map.add_argument(
		"--hdfs",
		action="store_true",
		help="cohort files are stored on the hdfs"
	)

	get_sample_name_map

	return parser.parse_args()
