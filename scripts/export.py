import hail as hl
from gnomad.utils.vcf import adjust_vcf_incompatible_types
from gnomad.utils.sparse_mt import default_compute_info


def mt_to_sites_only_ht(mt: hl.MatrixTable, n_partitions: int, densify: bool=False) -> hl.Table:
    """
    Convert matrix table (mt) into sites-only VCF-ready table (ht)
    """
    # Filter rows and add tags
    tmp_mt = mt
    # Densify
    if densify:
        tmp_mt = hl.experimental.densify(tmp_mt)
    
    tmp_mt = tmp_mt.filter_rows(
        (hl.len(tmp_mt.alleles) > 1) & (hl.agg.any(tmp_mt.LGT.is_non_ref()))
    )

    tmp_mt = tmp_mt.annotate_rows(
        site_dp=hl.agg.sum(tmp_mt.DP)
    )

    tmp_mt = tmp_mt.annotate_rows(
        ANS=hl.agg.count_where(hl.is_defined(tmp_mt.LGT)) * 2
    )

    # Create info table
    info_ht = default_compute_info(
        tmp_mt,
        site_annotations=True,
        n_partitions=n_partitions
    )

    info_ht = info_ht.annotate(
        info=info_ht.info.annotate(
            DP=tmp_mt.rows()[info_ht.key].site_dp
        )
    )

    info_ht = adjust_vcf_incompatible_types(info_ht, pipe_delimited_annotations=[])

    return info_ht


def mt_to_sites_only_vcf(mt: hl.MatrixTable, out_path: str, n_partitions: int, densify: bool=False):
    """
    Take a matrix table and export a sites-only VCF
    """
    ht = mt_to_sites_only_ht(mt=mt, n_partitions=n_partitions, densify=densify)
    hl.export_vcf(ht, out_path)


def vds_to_sites_only_vcf(vds: hl.vds.VariantDataset, out_path: str, n_partitions: int):
    """
    Take a variant dataset (VDS) and export a sites-only VCF
    """
    mt = hl.vds.to_dense_mt(vds)
    mt_to_sites_only_vcf(mt=mt, out_path=out_path, n_partitions=n_partitions, densify=False)