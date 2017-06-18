
from imports import *

DOCKER_DIR = '/gpfs/dockerimages/docker'
IMAGE_ROOT = os.path.join(DOCKER_DIR, 'image/aufs')
IMAGE_STORE_DIR = os.path.join(IMAGE_ROOT, 'imagedb')
LAYER_STORE_DIR = os.path.join(IMAGE_ROOT, 'layerdb')
AUFS_ROOT = os.path.join(DOCKER_DIR, 'aufs')
AUFS_LAYER_DIR = os.path.join(AUFS_ROOT, 'layers')
AUFS_MNT_DIR = os.path.join(AUFS_ROOT, 'mnt')
AUFS_DIFF_DIR = os.path.join(AUFS_ROOT, 'diff')
CONTANER_DIR = os.path.join(DOCKER_DIR, 'containers')

db_filename = 'database_json'
