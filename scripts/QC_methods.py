#---------------------------------------------------#
#                 Hail QC methods                   #
#---------------------------------------------------#

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

    # Path to input mt
    path_to_mt = prepend_location + os.path.abspath(parser.path_to_mt)

    # Path to output mt
    path_to_output = prepend_location + os.path.abspath(parser.output)

    logging.info("Initialising Hail")
    hl.init(
        app_name=parser.app_name,
        log=os.getcwd()
    )

    # Set choice for overwriting output
    overwrite_choice = False
    if parser.overwrite == True:
        overwrite_choice = True

    # Read in mt
    mt = hl.read_matrix_table(path_to_mt)

    # Split multiallelic sites
    mt = hl.split_multi_hts(mt)    

    # Generate GT field if it's not present. This is needed for sample QC.
    if "GT" not in mt.entry and "LGT" in mt.entry and "LA" in mt.entry:
        mt = mt.transmute_entries(GT = hl.experimental.lgt_to_gt(mt.LGT, mt.LA))

    # Run sample_qc, resulting field "sample_qc"
    mt = hl.sample_qc(mt, "sample_qc")

    # Run variant_qc, resulting field "variant_qc"
    mt = hl.variant_qc(mt, "variant_qc")

    logging.info("Completed sample and variant QC")

    # Run impute_sex
    impute_sex_mt = hl.impute_sex(mt.GT)

    logging.info("Completed impute_sex")

    # Adding the impute_sex fields to our original mt
    mt = mt.annotate_cols(impute_sex = impute_sex[MT.s])

    # Write mt to disk
    mt.write(path_to_output, overwrite=overwrite_choice)

    logging.info(f"Successfully wrote mt to {path_to_output}")

def init_log(path_to_output):
    now = datetime.now()
    dt_string = now.strftime("%d_%m_%Y_%I_%M_%S_%p")
    log_location = os.path.join(os.path.dirname(path_to_output), f"QC_methods_{dt_string}.log")

    logging.basicConfig(
        filename=log_location, 
        format="%(asctime)s %(message)s", 
        datefmt="%d/%m/%Y %I:%M:%S %p - ",
        level=logging.INFO
    )


if __name__ == "__main__":
    main()