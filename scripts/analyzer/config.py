
import sys
sys.path.append('../libraries/')
from regular_libraries import *


"""configurations for setup"""

dest_dirname = "/gpfs/docker_images_largefs"
"""mount tmpfs"""
extracting_dir = "/mnt/extracting_dir"
"""mount flash array"""
#extracting_dir = "/mnt/largessd"
analyzed_absfilename = '/gpfs/docker_images_largefs/results/analyzed_layer_file-less-1g.out'
layer_list_absfilename = '/gpfs/docker_images_largefs/job_list_dir/list_less_1g.out'
# list_less_1g.out
# list_less_2g.out
# list_bigger_2g.out
"""configurations for multithreading"""

num_worker_process = 20

"""contains all the global variables"""
dest_dir = []


"""Output directories which are no need to change"""
layer_db_json_dirname = 'layer_db_json'
image_db_json_dirname = 'image_db_json'
job_list_dirname = 'job_list_dir'

# q_dir_layers = []  # multiprocessing.Queue()

layer_config_map_dir_filename = "layer_config_map_dir.json"
layer_json_map_dir_filename = "layer_json_map_dir.json"
manifest_map_dir_filename = "manifest_map_dir.json"

dirs = ["/gpfs/docker_images_largefs/manifests", "/gpfs/docker_images_largefs/configs",
        "/gpfs/docker_images_largefs/layers", "/gpfs/docker_images_largefs/layer_db_json_bison02", 
	"/gpfs/docker_images_largefs/layer_db_json"]

# q_dir_images = multiprocessing.Queue()
# q_layer_json_db = multiprocessing.Queue()
#
# q_analyzed_images = multiprocessing.Queue()
# q_bad_unopen_image_manifest = multiprocessing.Queue()
# q_manifest_list_image = multiprocessing.Queue()
#
# q_flush_analyzed_images = multiprocessing.Queue()
#
# lock_f_bad_unopen_image_manifest = multiprocessing.Lock()
# lock_f_analyzed_image_manifest = multiprocessing.Lock()
# lock_f_manifest_list_image = multiprocessing.Lock()
# lock_q_analyzed_image_manifest = multiprocessing.Lock()
#
# lock_repo = multiprocessing.Lock()
"""files for plotting"""
image_pop_filename = ''
image_growth_filename = ''
# ===============================================

me = magic.Magic()
# me = magic.Magic(mime = True)
# DOCKER_DIR = '/gpfs/dockerimages/docker'
# IMAGE_ROOT = os.path.join(DOCKER_DIR, 'image/aufs')
# IMAGE_STORE_DIR = os.path.join(IMAGE_ROOT, 'imagedb')
# LAYER_STORE_DIR = os.path.join(IMAGE_ROOT, 'layerdb')
# AUFS_ROOT = os.path.join(DOCKER_DIR, 'aufs')
# AUFS_LAYER_DIR = os.path.join(AUFS_ROOT, 'layers')
# AUFS_MNT_DIR = os.path.join(AUFS_ROOT, 'mnt')
# AUFS_DIFF_DIR = os.path.join(AUFS_ROOT, 'diff')
# CONTANER_DIR = os.path.join(DOCKER_DIR, 'containers')
# db_filename = 'database_json'
