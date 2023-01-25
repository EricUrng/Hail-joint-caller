import hail as hl
from gnomad.utils.sparse_mt import split_info_annotation


def load_as_vqsr_vcf(as_vqsr_site_only_vcf_path: str, out_path: str, ref: str='GRCh38', overwrite: bool=True) -> hl.Table:
    """
    Loads an allele-specific (AS) VQSR site-only VCF into a site-only hail table
    """
    # import the vcf
    mt = hl.import_vcf(
        as_vqsr_site_only_vcf_path,
        force_bgz=True,
        reference_genome=ref,
    )
    # extract the rows into a hail table
    ht = mt.rows()
    # some numeric fields are loaded as strings, so convert them to ints and floats 
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

    # get the number of variants with VQSR annotations (multiallelics unsplit)
    unsplit_count = ht.count()

    # split multiallelics
    ht = hl.split_multi_hts(ht)
    ht = ht.annotate(
        info=ht.info.annotate(**split_info_annotation(ht.info, ht.a_index)),
    )
    ht = ht.annotate(
        filters=ht.filters.union(hl.set([ht.info.AS_FilterStatus])),
    )

    if out_path:
        ht.write(out_path, overwrite=overwrite)
        print("AS-VQSR site-only hail table written to disk.")

    # get the number of variants with VQSR annotations (multiallelics split)
    split_count = ht.count()

    print(f'Found {unsplit_count} unsplit and {split_count} split variants with VQSR annotations')
    return ht


def add_vqsr_to_mt(mt: hl.MatrixTable, vqsr_ht: hl.Table, out_path: str, densify=False, overwrite=True) -> hl.MatrixTable:
    # split multiallelics in MatrixTable
    tmp_mt = mt.annotate_rows(
        n_unsplit_alleles=hl.len(tmp_mt.alleles),
        mixed_site=(hl.len(tmp_mt.alleles) > 2)
        & hl.any(lambda a: hl.is_indel(tmp_mt.alleles[0], a), tmp_mt.alleles[1:])
        & hl.any(lambda a: hl.is_snp(tmp_mt.alleles[0], a), tmp_mt.alleles[1:]))
    tmp_mt = hl.experimental.sparse_split_multi(tmp_mt, filter_changed_loci=True)

    if densify:
        tmp_mt = hl.experimental.densify(tmp_mt)

    # annotate mt with vqsr
    tmp_mt = tmp_mt.annotate_rows(**vqsr_ht[tmp_mt.row_key])
    
    # vqsr_ht has info annotation split by allele; plus new AS-VQSR annotations
    tmp_mt = tmp_mt.annotate_rows(info=vqsr_ht[tmp_mt.row_key].info)

    # populating filters which is outside of info
    tmp_mt = tmp_mt.annotate_rows(
        filters=tmp_mt.filters.union(vqsr_ht[tmp_mt.row_key].filters),
    )
    
    tmp_mt = tmp_mt.annotate_globals(**vqsr_ht.index_globals())

    if out_path:
        tmp_mt.write(out_path, overwrite=overwrite)
    
    return tmp_mt


def add_vqsr_to_vds(vds: hl.vds.VariantDataset, vqsr_ht: hl.Table, out_path: str, densify=False, overwrite=True) -> hl.MatrixTable:
    if densify:
        mt = hl.vds.to_dense_mt(vds)
    else:
        mt = hl.vds.to_merged_sparse_mt(vds)
    return add_vqsr_to_mt(mt=mt, vqsr_ht=vqsr_ht, out_path=out_path, densify=False, overwrite=overwrite)