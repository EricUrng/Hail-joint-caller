import os
import sys
from cli import make_argparser
from run_combiner import run_combiner
from get_sample_name_map import get_sample_name_map

def main(): 
	parser = make_argparser()

	if parser.command == "joint_call":
		# print("RUNNING JOINT CALL")
		print(run_combiner())
	elif parser.command == "get_sample_name_map":
		get_sample_name_map(parser)
		# get_sample_name_map(parser)
	elif parser.command == "complete_QC":
		pass
	else:
		# parser.print_help(sys.stderr)
		pass

if __name__ == "__main__":
	main()
