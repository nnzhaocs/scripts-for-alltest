
from imports import *
from utility import *
from draw_pic import *

"""
        layer = {
            'layer_id': sha+':'+id,  # str(layer_id).replace("/", ""),
            'dirs': sub_dirs,  # getLayersBychainID(chain_id),
            'layer_depth': dir_depth,
            'size': size,  # sum of files size,
            'repeats': 0,
            'file_cnt': sum_file_cnt(sub_dirs)
        }
        
        size = {
            'uncompressed_sum_of_files': sum_layer_size(sub_dirs),
            'compressed_size_with_method_gzip': compressed_size_with_method_gzip,
            'archival_size': archival_size  # archival_size
        }
        
        dir_depth = {
            'dir_max_depth': max(depths),
            'dir_min_depth': min(depths),
            'dir_median_depth': median(depths),
            'dir_avg_depth': mean(depths)
        }
        
        sub_dir = {
                'dirname': tarinfo.name,  # .replace(layer_dir, ""),
                'dir_depth': dir_level
                # 'file_cnt': len(s_dir_files),
                # 'files': s_dir_files,  # full path of f = dir/files
                # 'dir_size': sum_dir_size(s_dir_files)
        }
        
        dir_file = {
            'filename': tarinfo.name,
            'sha256': sha256,
            # 'size (B)': None,
            'type': f_type,
            'extension': extension
            # 'symlink': symlink,
            # 'statinfo': statinfo
        }
        
        tar_info = {
            # 'st_nlink': stat.st_nlink,
            'ti_size': tarinfo.size,
            'ti_type': get_tarfile_type(tarinfo.type),
            'ti_uname': tarinfo.uname,
            'ti_gname': tarinfo.gname,
            # 'ti_atime': None,  # most recent access time
            'ti_mtime': tarinfo.mtime,  # change of content
            # 'ti_ctime': None  # matedata modify
            'link': link
            # 'hardlink': hardlink
        }
        
        dir['files'] = dir_files[dir['dirname']]
        dir['file_cnt'] = len(dir['files'])
        dir['dir_size'] = sum_dir_size(dir['files'])
"""


def layer_distribution():

    """
layer_info = {
        size: {
            'uncompressed_sum_of_files': sum_layer_size(sub_dirs),
            'compressed_size_with_method_gzip': compressed_size_with_method_gzip,
            'archival_size': archival_size  # archival_size
        }
        
        dir_depth = {
            'dir_max_depth': max(depths),
            'dir_min_depth': min(depths),
            'dir_median_depth': median(depths),
            'dir_avg_depth': mean(depths)
        }
        'file_cnt': sum_file_cnt(sub_dirs)
        
    }"""

    layer_base_info = {}
    size = defaultdict(list)
    dir_depth = defaultdict(list)
    file_cnt = []

    layer_dir = dest_dir[0]['layer_db_json_dir']
    for path, _, layer_json_filenames in os.walk(layer_dir):
        for layer_json_filename in layer_json_filenames:
            if not os.path.isfile(os.path.join(path, layer_json_filename)):
                logging.debug("layer json file %s is not valid!", layer_json_filename)
                continue
                # return None
            with open(os.path.join(path, layer_json_filename)) as lj_f:
                json_data = json.load(lj_f)

                size['uncompressed_sum_of_files'].append(json_data['size']['uncompressed_sum_of_files'] / 1024 / 1024)
                size['compressed_size_with_method_gzip'].append(json_data['size']['compressed_size_with_method_gzip'] / 1024 / 1024)
                size['archival_size'].append(json_data['size']['archival_size'] / 1024 / 1024)

                dir_depth['dir_max_depth'].append(json_data['layer_depth']['dir_max_depth'])

                file_cnt.append(json_data['file_cnt'])

    layer_base_info['size'] = size
    layer_base_info['dir_depth'] = dir_depth
    layer_base_info['file_cnt'] = file_cnt

    fig = fig_size('small')
    data1 = layer_base_info['size']['uncompressed_sum_of_files']
    xlabel = 'layer size (MB)'
    xlim = max(data1)
    ticks = 50
    print xlim
    plot_cdf(fig, data1, xlabel, xlim, ticks)
