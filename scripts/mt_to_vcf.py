import hail as hl
import os
import argparse
import logging

from gnomad.utils.vcf import adjust_vcf_incompatible_types
from gnomad.utils.sparse_mt import default_compute_info

def make_argparser():
    parser = argparse.ArgumentParser(
        description = "A tool for converting a Hail mt to a VCF file"
    )

    parser.add_argument(
        "path_to_mt",
        metavar="path_to_mt",
        type=str,
        help="path to the mt"
    )

    parser.add_argument(
        "output",
        metavar="path_to_output",
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
        "--app-name",
        metavar="app_name",
        type=str,
        help="name of the application"
    )

    return parser.parse_args()

def main():

    parser = make_argparser()
    path_to_mt = "file://" + os.path.abspath(parser.path_to_mt)
    path_to_output = "file://" + os.path.abspath(parser.output)

    print(path_to_mt)
    print(path_to_output)
    print(os.path.dirname(path_to_output))

    if parser.app_name:
        name = parser.app_name
    else:
        name = "Hail"

    hl.init(
        app_name=name,
        log=os.getcwd()
    )


    mt = hl.read_matrix_table(path_to_mt)

    mt = hl.experimental.densify(mt)

    mt = mt.filter_rows((hl.len(mt.alleles) > 1) & (hl.agg.any(mt.LGT.is_non_ref())))

    mt = mt.annotate_rows(site_dp=hl.agg.sum(mt.DP))

    mt = mt.annotate_rows(ANS=hl.agg.count_where(hl.is_defined(mt.LGT)) * 2)

    info_ht = default_compute_info(mt, site_annotations=True, n_partitions = parser.n_partitions)

    info_ht = info_ht.annotate(info=info_ht.info.annotate(DP=mt.rows()[info_ht.key].site_dp))

    ht = adjust_vcf_incompatible_types(
        info_ht, 
        pipe_delimited_annotations=[]
    )

    hl.export_vcf(ht, path_to_output)




if __name__ == "__main__":
    main()