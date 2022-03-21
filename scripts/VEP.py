#---------------------------------------------------#
#              Running VEP with Hail                #
#---------------------------------------------------#

from datetime import datetime
import argparse
import os
import logging
import hail as hl

def make_argparser():

    parser = argparse.ArgumentParser(
        description = "A tool for utilising VEP within Hail"
    )

    parser.add_argument(
        "path_to_mt",
        metavar="path_to_mt",
        type=str,
        help="path to the mt"
    )

    parser.add_argument(
        "output",
        metavar="output",
        type=str,
        help="path to the output file"
    )

    parser.add_argument(
        "config",
        metavar="config",
        type=str,
        help="path to the config file"
    )

    parser.add_argument(
        "--app_name",
        metavar="app_name",
        default="vep",
        type=str,
        help="name of the application"
    )

    parser.add_argument(
        "--hdfs",
        action="store_true",
        help="files are stored on the hdfs"
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="overwrite output file if it exists"
    )

    return parser.parse_args()

def main():
    parser = make_argparser()

    # Initialise logger
    init_log(os.path.abspath(parser.output))

	# Prepend for where files are stored. Local is default.
    prepend_location = "file://"
    if parser.hdfs == True:
        prepend_location = "hdfs://"

    # Set choice for overwriting output
    overwrite_choice = False
    if parser.overwrite == True:
        overwrite_choice = True

    # Path to input mt
    path_to_mt = prepend_location + os.path.abspath(parser.path_to_mt)

    # Path to output mt
    path_to_output = prepend_location + os.path.abspath(parser.output)

    # Path to config file
    path_to_config = prepend_location + os.path.abspath(parser.config)
    
    hl.init(
        app_name=parser.app_name,
        log=os.getcwd() + '/vep.log'
    ) 

    # Read in mt
    mt = hl.read_matrix_table(path_to_mt)

    # Filter star alleles out. Keeping only SNPs and indels
    mt = hl.filter_alleles(
        mt, 
        lambda allele, 
        i: hl.is_snp(mt.alleles[0], allele) | hl.is_indel(mt.alleles[0],allele)
    )

    # Run VEP
    mt = hl.vep(mt, path_to_config)

    # Write mt to disk
    mt.write(path_to_output, overwrite=overwrite_choice)

def init_log(path_to_output):
    now = datetime.now()
    dt_string = now.strftime("%d_%m_%Y_%I_%M_%S_%p")
    log_location = os.path.join(os.path.dirname(path_to_output), f"VEP_{dt_string}.log")

    logging.basicConfig(
        filename=log_location, 
        format="%(asctime)s %(message)s", 
        datefmt="%d/%m/%Y %I:%M:%S %p - ",
        level=logging.INFO
    )

if __name__ == "__main__":
    main()
