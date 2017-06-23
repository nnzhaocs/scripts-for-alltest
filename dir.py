
from imports import *
from draw_pic import *
from utility import *
from file import *


def clear_dirs(layer_id):
    """load the layer dir in layer file dest_dir['layer_dir']/<cache_id>/
    extracting the dir
    load all the subdirs in this layer dir """
    layer_file = os.path.join(dest_dir[0]['layer_dir'], layer_id)
    layer_dir = str(layer_file) + '-dir'
    cmd4 = 'rm -rf %s' % layer_dir
    logging.debug('The shell command: %s', cmd4)
    rc = os.system(cmd4)
    assert (rc == 0)


def load_dirs(layer_id):
    """load the layer file in layer file dest_dir['layer_dir']/<layer_id>
    extracting to id-dir
    load all the subdirs in this layer-id dir
    Then clean the layer-id dir"""
    sub_dirs = []
    # if len(layer_id) == 0:
    #     return sub_dirs

    layer_file = os.path.join(dest_dir[0]['layer_dir'], layer_id)
    if not os.path.isfile(layer_file):
        logging.warn('no following layer file for %s', layer_file)
        return sub_dirs

    logging.debug('Extracting the file ==========> %s' % layer_file)

    layer_dir = str(layer_file) + '-dir'
    cmd1 = 'mkdir %s' % layer_dir
    logging.debug('The shell command: %s', cmd1)
    rc = os.system(cmd1)
    assert (rc == 0)

    cmd = 'tar -zxf %s -C %s' % (layer_file, layer_dir)
    logging.debug('The shell command: %s', cmd)
    rc = os.system(cmd)
    assert (rc == 0)

    layer_dir_level = layer_dir.count(os.sep)
    logging.debug("(%s, %s)", layer_dir, layer_dir_level)

    if not os.path.isdir(layer_dir):
        logging.warn('no following layer dir for %s', layer_dir)
        return sub_dirs

    for path, subdirs, files in os.walk(layer_dir):
        for dirname in subdirs:
            s_dir = os.path.join(path, dirname)
            if not os.path.isdir(s_dir):
                logging.warn('no following layer subdir for %s', s_dir)
                continue

            dir_level = s_dir.count(os.sep) - layer_dir_level
            s_dir_files = []
            for f in os.listdir(s_dir):
                if os.path.isfile(os.path.join(s_dir, f)):
                    s_dir_file = load_file(os.path.join(s_dir, f))
                    s_dir_files.append(s_dir_file)
            # s_dir_files = [f for f in os.listdir(s_dir) if os.path.isfile(os.path.join(s_dir, f))]

            sub_dir = {
                'subdir': s_dir.replace(layer_dir, ""),
                'dir_depth': dir_level,
                'file_cnt': len(s_dir_files),
                'files': s_dir_files # full path of f = dir/files
            }

            logging.debug('sub_dir: %s', sub_dir)
            sub_dirs.append(sub_dir)
    return sub_dirs



# def load_files_to_layers(aufspath):
#     """get all files from multiple layer dirs = root_layer_dir"""
#     layers = []
#
#     folders = ([name for name in os.listdir(aufspath)
#         if os.path.isdir(os.path.join(aufspath, name)) and "init" not in name])
#     #cnt = 0
#     for folder in folders:
#         dirs = []
#         fs = []
# 	#cnt = cnt + 1
# 	#if cnt > 3:
# 	#	break
#         for path, subdirs, files in os.walk(os.path.join(aufspath, folder)):
#             for filename in files:
#                 f = os.path.join(path, filename)
# 		print f
#                 fs.append(f)
#
#             for dirname in subdirs:
#                 dir = os.path.join(path, dirname)
# 		print dir
#                 dirs.append(dir)
#
#         layer = {
#             'layer_cache_id': folder,
#             'dirs': dirs,
#             'files': fs
#         }
#
#         layers.append(layer)
#
#     return layers

# def get_union_files(layers):
#     files = []
#     for layer in layers:
#         files.extend(layer['files'])
#
#     return files
#
# def get_unique_files(all_files):
#     unique_files = []
#     #unique_cnt = 0
#     is_redundant_file = False
#     count = len(all_files)
#     for i in range(count):
#         for j in range(count - 1, i, -1):
#             file_cmp = filecmp.cmp(all_files[i], all_files[i + 1])
#             if file_cmp:
#                 """ture == equal"""
#                 is_redundant_file = True
#         if not is_redundant_file:
#             #unique_cnt = unique_cnt + 1
#             unique_files.append(all_files[i])
#
#     return unique_files
#
# def get_shared_files(all_files_temp):
#     all_files = all_files_temp
#     shared_files = []
#     is_redundant_file = False
#     #shared_cnt = 0
#
#     count = len(all_files)
#     for i in range(count):
#         shared_cnt = 1
#         if not all_files[i]:
#             continue
#         for j in range(count - 1, i, -1):
#             if not all_files[i + 1]:
#                 continue
#             file_cmp = filecmp.cmp(all_files[i], all_files[i + 1])
#             if file_cmp:
#                 """ture == equal"""
#                 is_redundant_file = True
#                 all_files[i + 1] = None
#                 shared_cnt = shared_cnt + 1
#
#         if is_redundant_file:
#             #shared_cnt = shared_cnt + 1
#             shared_files.append([all_files[i], shared_cnt])
#
#     return shared_files
