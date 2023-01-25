import hail as hl


def run_qt(mt: hl.MatrixTable, out_path: str) -> hl.MatrixTable:
    tmp_mt = mt
    # Generate GT field if it's not present. This is needed for sample QC.
    if "GT" not in tmp_mt.entry and "LGT" in tmp_mt.entry and "LA" in tmp_mt.entry:
        tmp_mt = tmp_mt.transmute_entries(GT = hl.experimental.lgt_to_gt(tmp_mt.LGT, tmp_mt.LA))

    # Split multiallelic sites
    tmp_mt = hl.split_multi_hts(tmp_mt)

    # Run sample_qc, resulting field "sample_qc"
    tmp_mt = hl.sample_qc(tmp_mt, "sample_qc")

    # Run variant_qc, resulting field "variant_qc"
    tmp_mt = hl.variant_qc(tmp_mt, "variant_qc")

    # Run impute_sex
    impute_sex_mt = hl.impute_sex(tmp_mt.GT)

    # Adding the impute_sex fields to our original mt
    tmp_mt = tmp_mt.annotate_cols(impute_sex = impute_sex_mt[tmp_mt.s])

    # Write mt to disk
    tmp_mt.write(out_path, overwrite=True)

    return tmp_mt