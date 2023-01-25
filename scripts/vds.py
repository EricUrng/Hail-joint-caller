import hail as hl
from os import path


def check_valid_cloud_storage_uris(uri_list: list=[]):
    # TODO
    return False


def check_valid_local_storage_paths(path_list: list=[]):
    return all([path.exists(f) for f in path_list])


def create_vds(out_path: str, tmp_path: str, ref: str='GRCh38', gvcf_list: list=[], vds_list: list=[], cloud_storage=False):
    # check inputs are lists
    if not isinstance(gvcf_list, list) or not isinstance(vds_list, list):
        raise ValueError("ERROR in create_vds: Parameters 'gvcf_list' and 'vds_list' must be lists of paths or URIs.")
    # check all gvcfs exist
    files_exist = False
    if cloud_storage:
        files_exist = check_valid_cloud_storage_uris(gvcf_list + vds_list)
    else:
        files_exist = check_valid_local_storage_paths(gvcf_list + vds_list)
    # determine gvcf and vds input arguments
    input_gvcf_list = gvcf_list if gvcf_list else None
    input_vds_list = vds_list if vds_list else None
    # create combiner
    combiner = hl.vds.new_combiner(
        output_path=out_path,
        temp_path=tmp_path,
        gvcf_paths=input_gvcf_list,
        vds_paths=input_vds_list,
        reference_genome=ref,
        use_exome_default_intervals=True
    )
    combiner.run()
    return hl.read_vds(out_path)