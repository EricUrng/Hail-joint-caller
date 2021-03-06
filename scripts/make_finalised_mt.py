#---------------------------------------------------#
#           Combine AS-VQSR ht with a mt            #
#---------------------------------------------------#

from datetime import datetime
import argparse
import os
import logging
import hail as hl

def make_argparser():
	
    parser = argparse.ArgumentParser(
        description = "A tool for generating the final annotated, soft-filtered Matrix Table"
    )

    parser.add_argument(
        "path_to_mt",
        metavar="path_to_mt",
        type=str,
        help="path to the matrix table generated by combiner"
    )
	
    parser.add_argument(
        "path_to_vqsr_ht",
        metavar="path_to_vqsr_ht",
        type=str,
        help="path to the VQSR ht"
    )

    parser.add_argument(
        "output",
        metavar="output",
        type=str,
        help="path to the final matrix table output destination"
    )

    parser.add_argument(
        "--app_name",
        metavar="app_name",
        default="make_finalised_mt",
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
		help="overwrite existing output file"
	)

    return parser.parse_args()

def main():
    parser = make_argparser()

	# Initialise logger
    init_log(os.path.abspath(parser.output))

	# Prepend location according for files. Default is local.
    prepend_location = "file://"
    if parser.hdfs == True:
        prepend_location = "hdfs://"

    # Path to input mt
    path_to_mt = prepend_location + os.path.abspath(parser.path_to_mt)

    # Path to input VQSR ht
    path_to_vqsr_ht = prepend_location + os.path.abspath(parser.path_to_vqsr_ht)

    # Path to output
    path_to_output = prepend_location + os.path.abspath(parser.output)

    # Initialise Hail
    hl.init(
        app_name=parser.app_name,
        log=os.getcwd()
    )

    # Read in input mt and AS_VQSR ht from previous steps
    mt = hl.read_matrix_table(path_to_mt)
    vqsr_ht = hl.read_table(path_to_vqsr_ht)

    # Annotating the rows of the mt with the site-only VQSR ht
    mt = annotate_vqsr(mt, vqsr_ht)

    # Creating new global fields by name from VQSR ht
    for ht in [vqsr_ht]:
        mt = mt.annotate_globals(**ht.index_globals())

    # Write mt to disk
    mt.write(path_to_output, overwrite=True)

def annotate_vqsr(mt, vqsr_ht):
    mt = mt.annotate_rows(**vqsr_ht[mt.row_key])
    
    mt = mt.annotate_rows(info=vqsr_ht[mt.row_key].info)
    
    mt = mt.annotate_rows(
        filters=mt.filters.union(vqsr_ht[mt.row_key].filters),
    )
    
    mt = mt.annotate_globals(**vqsr_ht.index_globals())
    
    return mt

def init_log(path_to_output):
    now = datetime.now()
    dt_string = now.strftime("%d_%m_%Y_%I_%M_%S_%p")
    log_location = os.path.join(os.path.dirname(path_to_output), f"make_finalised_mt{dt_string}.log")

    logging.basicConfig(
        filename=log_location, 
        format="%(asctime)s %(message)s", 
        datefmt="%d/%m/%Y %I:%M:%S %p - ",
        level=logging.INFO
    )



if __name__ == "__main__":
    main()