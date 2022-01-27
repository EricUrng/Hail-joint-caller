import argparse
import os

def make_argparser():
	
	parser = argparse.ArgumentParser(
		description = "A tool utilising Hail for joint variant calling"
	)
	
	subparsers = parser.add_subparsers(help="Sub-command help")

	get_sample_name_map = subparsers.add_parser(
		"get-sample-name-map",
		help="generate file list of entire cohort"
	)
