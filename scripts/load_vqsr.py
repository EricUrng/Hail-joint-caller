#---------------------------------------------------#
#           Convert AS-VQSR file to a ht            #
#---------------------------------------------------#

from datetime import datetime
import argparse
import os
import logging
import hail as hl

from gnomad.utils.sparse_mt import split_info_annotation

def make_argparser():

    parser = argparse.ArgumentParser(
        description = "A tool for converting a VQSR file to a ht"
    )

    parser.add_argument(
        "path_to_vqsr",
        metavar="path_to_vqsr",
        type=str,
        help="path to the VQSR file of your mt"
    )
	
    parser.add_argument(
        "output",
        metavar="output",
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

    parser.add_argument(
        "--hdfs",
        action="store_true",
        help="files are stored on the hdfs"
    )

    return parser.parse_args()

def main():
    parser = make_argparser()

	# Initialise logger
    init_log()
    # init_log(os.path.abspath(parser.output))

	# Prepend location according for files. Default is local.
    # prepend_location = "file://"
    # if parser.hdfs == True:
     #    prepend_location = "hdfs://"

	# Set choice for overwriting output
    overwrite_choice = False
    if parser.overwrite == True:
        overwrite_choice = True

    # Path to input mt
    path_to_vqsr = parser.path_to_vqsr
    # path_to_vqsr = prepend_location + os.path.abspath(parser.path_to_vqsr)

    # Path to output ht
    path_to_output = parser.output
    # path_to_output = prepend_location + os.path.abspath(parser.output)

	# Check if output exists already in case of overwriting
	# if os.path.exists(path_to_output) and overwrite_choice == False:
	# 	logging.info(
	# 		f"Output file {path_to_output} exists, use --overwrite to overwrite"
	# 	)
	# 	return

    # Initialise Hail  
    logging.info(f"Initialising Hail")
    hl.init(
        app_name=parser.app_name,
        log=os.path.join(os.getcwd(), 'log')
    )

    # Import the mt which was input for VQSR
    logging.info(f"Importing mt: {path_to_vqsr}")
    mt = hl.import_vcf(
        path_to_vqsr,
        reference_genome=parser.reference,
        force_bgz=True
    )

    # ht will contain an info struct which is equal to the info struct of mt
    # but values from fields AS_VQSLOD and AS_SB_TABLE are remapped to
    # integers and floats
    logging.info(f"Creating new info struct and remapping VQSR fields")
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
   
    logging.info(f"Splitting multi-allelic sites, filtering and annotating ht")

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
    logging.info(f"Writing ht to: {path_to_output}")
    ht.write(path_to_output, overwrite=overwrite_choice)

def init_log():
    now = datetime.now()
    dt_string = now.strftime("%d_%m_%Y_%I_%M_%S_%p")
    log_location = os.path.join(os.path.dirname(os.path.realpath(__file__)), f"load_vqsr{dt_string}.log")

    logging.basicConfig(
        filename=log_location, 
        format="%(asctime)s %(message)s", 
        datefmt="%d/%m/%Y %I:%M:%S %p - ",
        level=logging.INFO
    )

if __name__ == "__main__":
    main()