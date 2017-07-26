
from imports import *

"""contains all the global variables"""

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
layer_db_json_name = 'layer_db_json_bison02'
dest_dir = []

num_worker_process = 30

num_worker_threads = 2
num_flush_threads = 3

# threads = []
# flush_threads = []


def chunks(l, n):
    return [l[i:i+n] for i in range(0, len(l), n)]
