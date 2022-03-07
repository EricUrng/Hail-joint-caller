from datetime import datetime
import argparse
import os
import logging
import hail as hl

def make_argparser():

	parser = argparse.ArgumentParser(
		description = "A tool for utilising Hail for QC"
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
        "--app_name",
        metavar="app_name",
        default="mt_to_vcf",
        type=str,
        help="name of the application"
    )

    parser.add_argument(
        "--hdfs",
        action="store_true",
        help="sample files are stored on the hdfs"
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

    # Path to input mt
    path_to_mt = prepend_location + os.path.abspath(parser.path_to_mt)

    # Path to output mt
    path_to_output = prepend_location + os.path.abspath(parser.output)

    logging.info("Initialising Hail")
    hl.init(
        app_name=parser.app_name,
        log=os.getcwd()
    )

    # Read in mt
    mt = hl.read_matrix_table(path_to_mt)

    # Run sample_qc, resulting field "sample_qc"
    mt = hl.sample_qc(mt, "sample_qc")

    # Run variant_qc, resulting field "variant_qc"
    mt = hl.variant_qc(mt, "variant_qc")

    # Run impute_sex
    mt = hl.impute_sex(mt.GT)

    # Run summarize_variants
    mt = hl.summarize_variants(mt)

    # For those methods that generate a new mt
    # Perform an annotate to join the two matrices together
    # This includes all relatedness functions: identity_by_descent, king, pc_relate

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