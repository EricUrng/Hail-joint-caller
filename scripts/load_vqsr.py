#---------------------------------------------------#
#             Combine VQSR file with MT             #
#---------------------------------------------------#


import os
import argparse
import logging
from datetime import datetime
import hail as hl

def make_argparser():

    parser = argparse.ArgumentParser(
        description = "A tool for converting a VQSR file to a ht"
    )

    parser.add_argument(
        "path_to_mt",
        metavar="path_to_mt",
        type=str,
        help="path to the input matrix table"
    )
	
    parser.add_argument(
        "output_ht_path",
        metavar="output_ht_path",
        type=str,
        help="path to final output destination"
    )

    parser.add_argument(
		"--reference",
		metavar="reference",
		default="GRCh38",
		type=str,
		help="human genome reference to be used"
	)

    parser.add_argument(
        "--app_name",
        metavar="app_name",
        default="load_vqsr",
        type=str,
        help="name of the application"
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="overwrite output file if it exists"
    )

    return parser.parse_args()

def main():

	# Initialise logger
	init_log(os.path.abspath(parser.output))

    parser = make_argparser()

	# Set name for the Hail application
	if parser.app_name:
		name = parser.app_name
	else:
		name = "load_vqsr"

	# Prepend location according for files. Default is local.
	if hasattr(parser, "on_hdfs"):
		prepend_location = "hdfs://"
	else:
		prepend_location = "file://"

	# Set choice for possibly overwriting output
    if hasattr(parser, "overwrite"):
        overwrite_choice = True
    else:
        overwrite_choice = False

    # Path variables 
    path_to_mt = prepend_location + os.path.abspath(parser.path_to_mt)
    output_ht_path = prepend_location + os.path.abspath(parser.output_ht_path)

    # Initialise Hail  
    hl.init(
        app_name=parser.app_name,
        log=os.getcwd()
    )

    # Import the mt which was input for VQSR
    mt = hl.import_vcf(
        path_to_mt,
        reference_genome=parser.reference,
        force_bgz=True
    )

    # ht will contain an info struct which is equal to the info struct of mt
    # but values from fields AS_VQSLOD and AS_SB_TABLE are remapped to
    # integers and floats
    ht = mt.rows()
    ht = ht.annotate(
        info=ht.info.annotate(
            AS_VQSLOD=ht.info.AS_VQSLOD.map(hl.float),
            AS_SB_TABLE=ht.info.AS_SB_TABLE.split(r'\|').map(
                lambda x: hl.if_else(
                    x == '', hl.missing(hl.tarray(hl.tint32)), x.split(',').map(hl.int)
                )
            ),
        ),
    )
   
    # Split multiallelic variants 
    ht = hl.split_multi_hts(ht)

    # Create info and filter structs within ht
    ht = ht.annotate(
            info=ht.info.annotate(**split_info_annotation(ht.info, ht.a_index)),
    )
    ht = ht.annotate(
        filters=ht.filters.union(hl.set([ht.info.AS_FilterStatus])),
    )

    # Write ht to disk
    ht.write(output_ht_path, overwrite=overwrite_choice)

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