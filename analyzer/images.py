
from imports import *
from draw_pic import *
from utility import *
# from layers import *

from imports import *
from draw_pic import *
from utility import *
from dir import *
from itertools import chain

"""TODO:
    # 1. check duplicated.
    # 2. mount tmpfs
    # 3. already has json file no need to extracting again
    1. Download schema list
    2. analyze schema list
"""


def create_image_db(analyzed_image_filename):
    """create layer database as a json file"""
    logging.info('=============> create_layer_db: create layer metadata json file <===========')

    queue_images(analyzed_image_filename, q_analyzed_images)

    f_analyzed_image = open(analyzed_image_filename, 'a+')
    f_bad_unopen_image = open("bad_unopen_image_manifest_list.out", 'a+')
    f_manifest_list_image = open("image_manifest_list_repos.out", 'a+')

    for i in range(num_worker_threads):
        t = threading.Thread(target=load_image_manifest)
        t.start()
        threads.append(t)

    for j in range(num_flush_threads):
        t1 = threading.Thread(target=flush_file, args=(f_analyzed_image, q_flush_analyzed_images, lock_f_analyzed_image_manifest))
        t2 = threading.Thread(target=flush_file, args=(f_bad_unopen_image, q_bad_unopen_image_manifest, lock_f_bad_unopen_image_manifest))
        t3 = threading.Thread(target=flush_file, args=(f_manifest_list_image, q_manifest_list_image, lock_f_manifest_list_image))

        t1.start()
        t2.start()
        t3.start()

        flush_threads.append(t1)
        flush_threads.append(t2)
        flush_threads.append(t3)

    q_dir_images.join()
    logging.info('wait queue to join!')
    for i in range(num_worker_threads):
        q_dir_images.put(None)
    logging.info('put none layers to queue!')
    for t in threads:
        t.join()
    logging.info('done! all the layer threads are finished')

    q_flush_analyzed_images.join()
    q_bad_unopen_image_manifest.join()
    q_manifest_list_image.join()

    print "flush queues wait here!"
    for i in range(num_flush_threads):
        q_flush_analyzed_images.put(None)
        q_bad_unopen_image_manifest.put(None)
        q_manifest_list_image.put(None)
    for t in flush_threads:
        t.join()


def queue_images(analyzed_filename, queue):
    """queue the layer id in downloaded_layer_filename, layer id = sha256:digest !!! without timestamp"""
    """queue the image in analyzed_filename, image name = usernamespace/repo"""
    with open(analyzed_filename) as f:
        for line in f:
            print line
            if line:
                logging.debug('queue layer_id: %s to analyzed_layer_queue', line.replace("\n", ""))  #
                queue.put(line.replace("\n", ""))

    """queue the manifest in dest_dir/manifests, manifest name = usrnamespace-repo-timestamp"""
    for path, _, manifest_filenames in os.walk(dest_dir[0]['manifest_dir']):
        for manifest_filename in manifest_filenames:
            logging.debug('manifest: %s', manifest_filename)  # str(layer_id).replace("/", "")
            q_dir_images.put(manifest_filename)
            logging.debug('queue dir layer tarball: %s', manifest_filename)  # str(layer_id).replace("/", "")

    """queue the manifest in dest_dir/layer_db_json_dir, layer db json name = sha256-digest-timestamp.json"""
    for path, _, layer_json_filenames in os.walk(dest_dir[0]['layer_db_json_dir']):
        for layer_json_filename in layer_json_filenames:
            logging.debug('manifest: %s', layer_json_filename)  # str(layer_id).replace("/", "")
            q_layer_json_db.put(layer_json_filename)
            logging.debug('queue dir layer tarball: %s', layer_json_filename)  # str(layer_id).replace("/", "")


def is_valid_manifest(manifest_filename):
    if len(manifest_filename.split("-")) != 3:
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
        for layer_json_filename in q_layer_json_db.queue:
            if sstr in layer_json_filename:
                layer_json_filenames.append(layer_json_filename)

    return layer_json_filenames


def load_image_manifest():  # dest_dir[0]['layer_db_json_dir']
    """load the image manifest dirs"""
    while True:
        manifest_filename = q_dir_images.get()
        if not is_valid_manifest(manifest_filename):
            q_dir_images.task_done()
            continue

        with lock_q_analyzed_image_manifest:
            if manifest_filename in q_analyzed_images.queue:
                print "Manifest Already Analyzed!"
                is_Manifest_analyzed = True
            else:
                is_Manifest_analyzed = False
                q_analyzed_layers.put(manifest_filename)
                print "Manifest Not Analyzed!"

        if is_Manifest_analyzed:
            q_dir_images.task_done()
            continue

        logging.debug('process manifest_filename: %s', manifest_filename)  # str(layer_id).replace("/", "")
        if manifest_filename is None:
            logging.debug('The dir image queue is empty!')
            break

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
            q_dir_images.task_done()
            continue

        if version == 'schema2':
            if len(layer_json_filenames) != len(blobs_digest) - 1:
                logging.info('layer_json_filenames Not enough!')
                q_bad_unopen_layers.put(manifest_filename)
                q_dir_images.task_done()
                continue
        elif version == 'schema1':
            if len(layer_json_filenames) != len(blobs_digest):
                logging.info('layer_json_filenames Not enough!')
                q_bad_unopen_layers.put(manifest_filename)
                q_dir_images.task_done()
                continue
        elif version == 'schemalist':
            logging.info('This is a schema list, will do it later!')
            q_manifest_list_image.put()
            q_dir_images.task_done()
            continue

        image = {
            'manifest version': version,  # str(layer_id).replace("/", ""),
            'tag': manifest_filename.split("-")[1],  # getLayersSize(chainid),
            'docker version': None,
            'os': None,
            'architecture': None,
            'pull_cnt': None,
            # ==================================================

            'size': 0,

            'pull_cnt': 0,
            'file_cnt': 0,
            'layer_cnt': len(blobs_digest),
            # ==================================================
            'layers': layer_db_json_data  # getLayersBychainID(chain_id),
        }

        absimage_filename = os.path.join(dest_dir[0]['image_db_json_dir'], manifest_filename+'.json')
        logging.info('write to image json file: %s', absimage_filename)
        with open(absimage_filename, 'w') as f_out:
            json.dump(image, f_out)

        logging.debug('write image:[%s]: to json file %s', manifest_filename, absimage_filename)
        q_flush_analyzed_layers.put(manifest_filename)
        q_dir_layers.task_done()


def flush_file(fd, q_name, lock_file):
    while True:
        item = q_name.get()
        if item is None:
            print str(q_name) + " queue empty!"
            break
        """write to file"""
        print "f_finished_item: " + item
        with lock_file:
            fd.write(item + "\n")
            fd.flush()
        q_name.task_done()





















