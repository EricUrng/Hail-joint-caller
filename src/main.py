import os
import sys
from cli import make_argparser
from run_combiner import run_combiner

def main(): 
	parser = make_argparser()
#	args = parse_args(parser)	

	if parser.command == "joint_call":
		print(run_combiner())
	elif parser.command == "get_sample_name_map":
		pass
	elif parser.command == "complete_QC":
		pass
	else:
		parser.print_help(sys.stderr)

if __name__ == "__main__":
	main()
