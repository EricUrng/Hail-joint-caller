import hail as hl


def run_vep(mt: hl.MatrixTable, vep_config: str, out_path: str) -> hl.MatrixTable:
    # filter star alleles out, keep only SNPs and INDELs
    tmp_mt = hl.filter_alleles(
        mt,
        lambda allele, i: hl.is_snp(mt.alleles[0], allele) | hl.is_indel(mt.alleles[0],allele)
    )

    # run VEP
    tmp_mt = hl.vep(tmp_mt, vep_config)

    if out_path:
        tmp_mt.write(out_path, overwrite=True)

    return tmp_mt