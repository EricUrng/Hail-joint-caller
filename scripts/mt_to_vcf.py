#-------------------------------------------------------------#
#              Convert a MT into a site-only VCF              #
#-------------------------------------------------------------#

from datetime import datetime
import argparse
import os
import logging
import hail as hl

from gnomad.utils.vcf import adjust_vcf_incompatible_types
from gnomad.utils.sparse_mt import default_compute_info

def make_argparser():
    parser = argparse.ArgumentParser(
        description = "A tool for converting a Hail mt to a VCF"
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
        "--n_partitions",
        metavar="n_partitions",
        default=5000,
        type=int,
        help="number of partitions for output matrix table"
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

    # Path to output VCF
    path_to_output = prepend_location + os.path.abspath(parser.output)

	# Check if output exists already in case of overwriting
	if os.path.exists(path_to_output) and overwrite_choice == False:
		logging.info(
			f"Output file {path_to_output} exists, use --overwrite to overwrite"
		)
		return

    logging.info("Initialising Hail")

    hl.init(
        app_name=parser.app_name,
        log=os.getcwd()
    )

    logging.info(f"Reading mt from {path_to_mt}")

    # Read in mt to be converted
    mt = hl.read_matrix_table(path_to_mt)

    logging.info('Converting mt to sites-only mt')

    # Reference blocks of the sparse matrix has to be expanded 
    mt = hl.experimental.densify(mt)

    # Filter for rows with at least one non-reference allele
    mt = mt.filter_rows((hl.len(mt.alleles) > 1) & (hl.agg.any(mt.LGT.is_non_ref())))

    # Annotate site level DP
    mt = mt.annotate_rows(site_dp=hl.agg.sum(mt.DP))

    # Annotate AN tag as ANS
    mt = mt.annotate_rows(ANS=hl.agg.count_where(hl.is_defined(mt.LGT)) * 2)

    # Generate ht with GATK allele-specific info fields
    info_ht = default_compute_info(mt, site_annotations=True, n_partitions = parser.n_partitions)
    info_ht = info_ht.annotate(info=info_ht.info.annotate(DP=mt.rows()[info_ht.key].site_dp))

    # Convert annotation into pipe-delimited strings
    # All int64 are coerced into int32
    ht = adjust_vcf_incompatible_types(
        info_ht, 
        pipe_delimited_annotations=[]
    )

    logging.info(f"Exporting sites-only VCF to {path_to_output}")

    # Exporting ht to VCF
    hl.export_vcf(ht, path_to_output)

    logging.info(f"Successfully exported")


def init_log(path_to_output):
    now = datetime.now()
    dt_string = now.strftime("%d_%m_%Y_%I_%M_%S_%p")
    log_location = os.path.join(os.path.dirname(path_to_output), f"mt_to_vcf_{dt_string}.log")

    logging.basicConfig(
        filename=log_location, 
        format="%(asctime)s %(message)s", 
        datefmt="%d/%m/%Y %I:%M:%S %p - ",
        level=logging.INFO
    )

if __name__ == "__main__":
    main()