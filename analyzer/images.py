
from imports import *
# from draw_pic import *
from utility import *
# from layers import *

from imports import *
from draw_pic import *
from utility import *
# from dir import *
from itertools import chain

"""TODO:
    # 1. check duplicated.
    # 2. mount tmpfs
    # 3. already has json file no need to extracting again
    1. Download schema list
    2. analyze schema list
"""


from imports import *
from utility import *
from draw_pic import *


q_analyzed_layer_jsons = []
q_dir_manifest_jsons = []


def create_image_db(args):
    """create image database as a json file"""
    logging.info('=============> create_image_db: create image metadata json file <===========')

    queue_images()

    print "create pool"
    P = multiprocessing.Pool(6)
    print "before map"
    P.map(load_image_manifest, q_dir_manifest_jsons)
    # P.close()
    # P.join()
    print "after map"


def queue_images():
    """queue the layer id in downloaded_layer_filename, layer id = sha256:digest !!! without timestamp"""
    """queue the image in analyzed_filename, image name = usernamespace/repo"""

    """queue the manifest in dest_dir/manifests, manifest name = usrnamespace-repo-timestamp"""
    logging.debug("start queue_images")
    i = 0
    for path, _, manifest_filenames in os.walk(dest_dir[0]['manifest_dir']):
        for manifest_filename in manifest_filenames:
            #logging.debug('manifest: %s', manifest_filename)  # str(layer_id).replace("/", "")
            i = i + 1 
            if i > 50:
                break               
            q_dir_manifest_jsons.append(manifest_filename)
            logging.debug('queue q_dir_manifest_jsons: %s', manifest_filename)  # str(layer_id).replace("/", "")

    """queue the manifest in dest_dir/layer_db_json_dir, layer db json name = sha256-digest-timestamp.json"""
    for path, _, layer_json_filenames in os.walk(dest_dir[0]['layer_db_json_dir']):
        for layer_json_filename in layer_json_filenames:
            #logging.debug('manifest: %s', layer_json_filename)  # str(layer_id).replace("/", "")
            q_analyzed_layer_jsons.append(layer_json_filename)
            logging.debug('queue q_analyzed_layer_jsons: %s', layer_json_filename)  # str(layer_id).replace("/", "")


def is_valid_manifest(manifest_filename):
    if len(manifest_filename.split("-")) < 2:
        logging.debug('The manifest filename is invalid %s!', manifest_filename)
        return False
    if not os.path.isfile(os.path.join(dest_dir[0]['manifest_dir'], manifest_filename)):
        logging.info('image manifest file %s is not valid', manifest_filename)
        return False
    return True


def manifest_schemalist(manifest):
    blobs_digest = []
    if 'manifests' in manifest and isinstance(manifest['manifests'], list) and len(manifest['manifests']) > 0:
        for i in manifest['manifests']:
            if 'digest' in i:
                print i['digest']
                blobs_digest.append(i['digest'])
    return blobs_digest


def manifest_schema2(manifest):
    blobs_digest = []
    if 'config' in manifest and 'digest' in manifest['config']:
        config_digest = manifest['config']['digest']
        blobs_digest.append(config_digest)
    if 'layers' in manifest and isinstance(manifest['layers'], list) and len(manifest['layers']) > 0:
        for i in manifest['layers']:
            if 'digest' in i:
                print i['digest']
                blobs_digest.append(i['digest'])
    return blobs_digest


def manifest_schema1(manifest):
    blobs_digest = []
    if 'fsLayers' in manifest and isinstance(manifest['fsLayers'], list) and len(manifest['fsLayers']) > 0:
        for i in manifest['fsLayers']:
            if 'blobSum' in i:
                print i['blobSum']
                blobs_digest.append(i['blobSum'])
    return blobs_digest


def find_layer_json_files(blobs_digest):
    layer_json_filenames = []
    if not len(blobs_digest):
        return layer_json_filenames

    for digest in blobs_digest:
        sstr = digest.replace(":", "-")
        for layer_json_filename in q_analyzed_layer_jsons:
            if sstr in layer_json_filename:
                layer_json_filenames.append(layer_json_filename)

    return layer_json_filenames


def load_image_manifest(manifest_filename):  # dest_dir[0]['layer_db_json_dir']
    """load the image manifest dirs"""
    if not is_valid_manifest(manifest_filename):
        return

    logging.debug('process manifest_filename: %s', manifest_filename)  # str(layer_id).replace("/", "")
    if manifest_filename is None:
        logging.debug('The dir image queue is empty!')
        return

    with open(os.path.join(dest_dir[0]['manifest_dir'], manifest_filename)) as manifest_file:
        layer_db_json_data = json.load(manifest_file)

    if 'schemaVersion' in layer_db_json_data and layer_db_json_data['schemaVersion'] == 2:
        if 'mediaType' in layer_db_json_data and 'list' in layer_db_json_data['mediaType']:
            blobs_digest = manifest_schemalist(layer_db_json_data)
            version = 'schemalist'
        else:
            blobs_digest = manifest_schema2(layer_db_json_data)
            version = 'schema2'

    elif 'schemaVersion' in layer_db_json_data and layer_db_json_data['schemaVersion'] == 1:
        blobs_digest = manifest_schema1(layer_db_json_data)
        version = 'schema1'

    logging.info('blobs_digests: %s', blobs_digest)

    layer_json_filenames = find_layer_json_files(blobs_digest)
    logging.info('layer_json_filenames: %s', layer_json_filenames)

    if not layer_json_filenames:
        logging.info('layer_json_filenames empty!')
        return

    if version == 'schema2':
        if len(layer_json_filenames) != len(blobs_digest) - 1:
            logging.info('layer_json_filenames Not enough!')
            return
            # q_bad_unopen_layers.put(manifest_filename)
            # q_dir_images.task_done()
            # continue
    elif version == 'schema1':
        if len(layer_json_filenames) != len(blobs_digest):
            logging.info('layer_json_filenames Not enough!')
            return
            # q_bad_unopen_layers.put(manifest_filename)
            # q_dir_images.task_done()
            # continue
    elif version == 'schemalist':
        logging.info('This is a schema list, will do it later!')
        return
        # q_manifest_list_image.put()
        # q_dir_images.task_done()
        # continue
    image_data = load_layer_jsons(layer_json_filenames)
    image_data_info = sum_image_info(image_data)
    size = {
	'uncompressed_sum_of_files': image_data_info['uncompressed_sum_of_files'],
	'compressed_size_with_method_gzip': image_data_info['compressed_size_with_method_gzip'],
	'archival_size': image_data_info['archival_size'],
	#'file_cnt': image_data_info['file_cnt']
    }
    image = {
        'manifest version': version,  # str(layer_id).replace("/", ""),
        'tag': 'latest',  # getLayersSize(chainid),
        #'docker version': None,
        #'os': None,
        #'architecture': None,
        #'pull_cnt': None,
        # ==================================================
        'size': size,
        # 'pull_cnt': 0,
        'file_cnt': image_data_info['file_cnt'],
        'layer_cnt': len(blobs_digest)
        # ==================================================
        #'layers': image_data #load_layer_jsons(layer_json_filenames)  # getLayersBychainID(chain_id),
    }
   
    absimage_filename = os.path.join(dest_dir[0]['image_db_json_dir'], manifest_filename+'.json')
    logging.info('write to image json file: %s', absimage_filename)
    with open(absimage_filename, 'w') as f_out:
        json.dump(image, f_out)

    logging.debug('wrote image:[%s]: to json file %s', manifest_filename, absimage_filename)
    del image_data
    del image_data_info
	 # q_flush_analyzed_layers.put(manifest_filename)
    # q_dir_layers.task_done()


def load_layer_jsons(layer_json_filenames):
    layer_json_datas = []
    print "load layer jsons"
    for layer_json_filename in layer_json_filenames:
        if os.path.join(dest_dir[0]['layer_db_json_dir'], layer_json_filename):
            with open(os.path.join(dest_dir[0]['layer_db_json_dir'], layer_json_filename), 'r') as l_f:
                layer_json_data = json.load(l_f)

            if layer_json_data:
                layer_json_datas.append(layer_json_data)
    return layer_json_datas


def sum_image_info(layer_json_datas):
    layer_base_info = {}
    layer_base_info['uncompressed_sum_of_files'] = 0
    layer_base_info['compressed_size_with_method_gzip'] = 0
    layer_base_info['archival_size'] = 0

    #layer_base_info['dir_max_depth'] = dir_max_depth
    layer_base_info['file_cnt'] = 0

    if not layer_json_datas:
        logging.debug("layer_json_datas is none!") 
    for json_data in layer_json_datas:
 	uncompressed_sum_of_files = json_data['size']['uncompressed_sum_of_files']
        compressed_size_with_method_gzip = json_data['size']['compressed_size_with_method_gzip']
        archival_size = json_data['size']['archival_size']

        dir_max_depth = json_data['layer_depth']['dir_max_depth']

        file_cnt = json_data['file_cnt']

    layer_base_info['uncompressed_sum_of_files'] = layer_base_info['uncompressed_sum_of_files'] + uncompressed_sum_of_files
    layer_base_info['compressed_size_with_method_gzip'] = layer_base_info['compressed_size_with_method_gzip'] + compressed_size_with_method_gzip
    layer_base_info['archival_size'] = layer_base_info['archival_size'] + archival_size

    #layer_base_info['dir_max_depth'] = dir_max_depth
    layer_base_info['file_cnt'] = layer_base_info['file_cnt'] + file_cnt

    return layer_base_info

