import hail as hl
import os
import argparse


def make_argparser():
    parser = parser.ArgumentParser(
        description = "A tool for converting a Hail mt to a VCF file"
    )

    parser.add_argument(
        "mt",
        metavar="path_to_mt",
        type=str,
        help="path to the mt"
    )

    parser.add_argument(
        "output",
        metavar="path_to_output",
        nargs="?",
        type=str,
        help="path to the output file"
    )

    parse_args.add_argument


    return parser

def main()

    parser = make_argparser()




if __name__ == "__main__":
    main()