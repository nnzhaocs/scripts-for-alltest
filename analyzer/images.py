
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
"""


def create_image_db(analyzed_image_filename):
    """create layer database as a json file"""
    logging.info('=============> create_layer_db: create layer metadata json file <===========')

    queue_images(analyzed_image_filename, q_analyzed_images)

    f_analyzed_image = open(analyzed_image_filename, 'a+')
    f_bad_unopen_image = open("bad_unopen_image_manifest_list.out", 'a+')

    for i in range(num_worker_threads):
        t = threading.Thread(target=load_image_manifest)
        t.start()
        threads.append(t)

    for j in range(num_flush_threads):
        t1 = threading.Thread(target=flush_file, args=(f_analyzed_image, q_flush_analyzed_images, lock_f_analyzed_image_manifest))
        t2 = threading.Thread(target=flush_file, args=(f_bad_unopen_image, q_flush_bad_unopen_image_manifest, lock_f_bad_unopen_image_manifest))
        t1.start()
        t2.start()
        flush_threads.append(t1)
        flush_threads.append(t2)

    q_dir_images.join()
    logging.info('wait queue to join!')
    for i in range(num_worker_threads):
        q_dir_images.put(None)
    logging.info('put none layers to queue!')
    for t in threads:
        t.join()
    logging.info('done! all the layer threads are finished')

    q_flush_analyzed_images.join()
    q_flush_bad_unopen_image_manifest.join()

    print "flush queues wait here!"
    for i in range(num_flush_threads):
        q_flush_analyzed_images.put(None)
        q_flush_bad_unopen_image_manifest.put(None)
    for t in flush_threads:
        t.join()

    # while not layer_q.empty():
    #     layer = layer_q.get()
    #     # if layer is None:
    #     #     break
    #     layers.append(layer)
        # layer_q.task_done()
    # json.dump(layers, f_layer_db)
    # f_out.close() , args=(dest_dir[0]['layer_db_json_dir'])


def queue_images(analyzed_filename, queue):
    """queue the layer id in downloaded_layer_filename, layer id = sha256:digest !!! without timestamp"""
    # with open(downloaded_layer_filename) as f:
    #     for line in f:
    #         print line
    #         if line:
    #             logging.debug('queue layer_id: %s to downloaded layer queue', line.replace("\n", ""))  #
    #             q_downloaded_layers.put(line.replace("\n", ""))
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
            # if len(layer_json_filename.split("-")) != 3:
            #     logging.debug('The layer filename is invalid %s!', layer_json_filename)
            #     continue
            # sstr = layer_json_filename.split("-")[1]
            logging.debug('manifest: %s', layer_json_filename)  # str(layer_id).replace("/", "")
            q_layer_json_db.put(layer_json_filename)
            logging.debug('queue dir layer tarball: %s', layer_json_filename)  # str(layer_id).replace("/", "")


# def check_config_file(filename):
#     tarball_filename = os.path.join(dest_dir[0]['layer_dir'], filename)
#     cmd2 = 'file %s' % tarball_filename
#     logging.debug('The shell command: %s', cmd2)
#
#     proc = subprocess.Popen(cmd2, stdout=subprocess.PIPE, shell=True)
#     out, err = proc.communicate()
#     logging.debug('The shell output: %s', out)
#     if 'gzip' in out:
#         return False
#     else:
#         return True
#
#
# def move_config_file(filename):
#     config_filename = os.path.join(dest_dir[0]['layer_dir'], filename)
#     cmd3 = 'mv %s %s' % (config_filename, dest_dir[0]['config_dir'])
#     logging.debug('The shell command: %s', cmd3)
#     # rc = os.system(cmd3)
#     # assert (rc == 0) # or use try exception
#     # # logging.debug('The shell output: %s', out)
#     try:
#         subprocess.check_output(cmd3, shell=True)
#     except subprocess.CalledProcessError as e:
#         print e.output
#
#
def is_valid_manifest(manifest_filename):
    # if "sha256-" not in layer_filename:
    #     logging.info('file %s is not a layer tarball or config file', layer_filename)
    #     return False
    # if len(layer_filename.split("-")) != 3:
    #     logging.debug('The layer filename is invalid %s!', layer_filename)
    #     return False
    if not os.path.isfile(os.path.join(dest_dir[0]['manifest_dir'], manifest_filename)):
        logging.info('image manifest file %s is not valid', manifest_filename)
        return False
    # if check_config_file(layer_filename):
    #     move_config_file(layer_filename)
    #     return False
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

        # any(layer_json_filename == sstr for layer_json_filename in q_layer_json_db.queue)


def manifest_schema1(manifest):
    blobs_digest = []
    if 'fsLayers' in manifest and isinstance(manifest['fsLayers'], list) and len(manifest['fsLayers']) > 0:
        for i in manifest['fsLayers']:
            if 'blobSum' in i:
                print i['blobSum']
                blobs_digest.append(i['blobSum'])
    return blobs_digest


def load_image_manifest(): # dest_dir[0]['layer_db_json_dir']
    """load the layer dirs"""
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

        # with lock_repo:
        #     if manifest_filename in q_analyzed_images.queue:
        #         print "Manifest Already Analyzed!"
        #         is_repo_exist = True
        #     else:
        #         is_repo_exist = False
        #         print "Manifest Not Analyzed!"

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
        if not layer_json_filenames:
            logging.info('layer_json_filenames empty!')
            q_dir_images.task_done()
            continue

        logging.info('layer_json_filenames: %s', layer_json_filenames)

        # with lock_q_analyzed_layer:
        # if ('sha256:' + layer_filename.split("-")[1]) in q_analyzed_layers.queue:
        #     print "Layer Already Analyzed!"
        #     is_layer_analyzed = True
        # else:
        #     is_layer_analyzed = False
        #     q_analyzed_layers.put('sha256:' + layer_filename.split("-")[1])
        #     print "Layer Not Analyzed!"

        # if is_layer_analyzed:
        #     q_dir_layers.task_done()
        #     continue

        # sub_dirs = load_dirs(layer_filename, extracting_dir)
        # if not len(sub_dirs):
        #     q_dir_layers.task_done()
        #     clear_dirs(layer_filename, extracting_dir)
        #     logging.debug('The dir wrong!')
        #     continue
        #
        # clear_dirs(layer_filename, extracting_dir)

        # depths = [sub_dir['dir_depth'] for sub_dir in sub_dirs if sub_dir]
        # if depths:
        #     dir_depth = max(depths)
        #     # print dir_depth
        # else:
        #     dir_depth = 0

        # sha, id, timestamp = str(layer_filename).split("-")

        image = {
            'version': version,  # str(layer_id).replace("/", ""),
            'layers': layer_db_json_data,  # getLayersBychainID(chain_id),
            'size': 0
            # 'size': 0,  # getLayersSize(chainid),
            # 'repeats': 0
        }

        # layer_q.put(layer)
        absimage_filename = os.path.join(dest_dir[0]['image_db_json_dir'], manifest_filename+'.json')
        logging.info('write to image json file: %s', absimage_filename)
        with open(absimage_filename, 'w') as f_out:
            json.dump(image, f_out)
        # lock.acquire()
        # layers.append(layer)
        # lock.release()

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

# def load_layerBychainid(chainid):
#     """ first we find the layer folder with chain id under /var/lib/docker/image/aufs/layerdb
#     then the file 'cache-id' has the contents of the real layer id
#     """
#     cache_file = os.path.join(LAYER_STORE_DIR, 'sha256', chainid, 'cache-id')
#     #print  os.path.join(LAYER_STORE_DIR, 'sha256', chainid, 'cache-id')
#     if not os.path.isfile(cache_file):
#         logging.info('no cache-id file found for this chain id: %s', chainid)
#         return []
#
#     with open(cache_file) as data_file:
#         cache_id = data_file.read()
#
#     if len(cache_id) == 0:
#         logging.info('cache-id file is empty, no layers for this chain id:%s' % chainid)
#         return []
#
#     logging.debug('found cache id for chain id %s -> %s' % (chainid, cache_id))
#
#     sub_dirs = load_dirs(cache_id)
#
#     depths = [sub_dir['dir_depth'] for sub_dir in sub_dirs if sub_dir]
#     if depths:
#         dir_depth = max(depths)
#         print dir_depth
#     else:
#         dir_depth = 0
#
#     layer = {
#         'chain_id': chainid,
#         'cache_id': cache_id,
#         'dirs': sub_dirs,  # getLayersBychainID(chain_id),
#         'dir_depth': dir_depth,
#         'size': getLayersSize(chainid),
#         'repeats': 0
#     }
#
#     #print layer
#     return layer

# def getLayersSize(chainid):
#     """ first we find the layer folder with chain id under /var/lib/docker/image/aufs/layerdb
#     then the file 'size' has size"""
#     size_file = os.path.join(LAYER_STORE_DIR, 'sha256', chainid, 'size')
#     #print  size_file
#     if not os.path.isfile(size_file):
#         logging.info('no size file found for this chain id: %s', chainid)
#         return []
#
#     with open(size_file) as data_file:
#         size = data_file.read()
#
#     if len(size) == 0:
#         logging.info('size file is empty, no layers for this chain id:%s' % chainid)
#         return []
#
#     logging.debug('found size for chain id %s -> %s' % (chainid, size))
#     return size


# def cal_layer_repeats(images):
#     """ [imge1's layers, image2's layers, ..... ]
#     get unique elements among multiple lists: all_list"""
#     #fout = open('layer-repeats.txt', 'w+')
#     layers = []
#     for image in images:
#         image_layers = []
#         for layer in image['layers']:
#             #print layer
#             diff_path = os.path.join(dest_dir['layer_dir'], layer['cache_id'])
#             logging.debug('%s', '\n'.join(diff_path))
#             """here, find all layer for each image"""
#             image_layers.append(layer['cache_id'])
#         layers.append(image_layers)
#     print layers[0]
#
#     layer_union = list(chain(*layers))
#     print layer_union[0]
#     layer_dict = {i:layer_union.count(i) for i in layer_union}
#     #layer_repeats_dict = cal_layer_repeats(images)
#     for k, v in layer_dict.items():
#         print (k, v)
#         #fout.writelines(str(k)+','+str(v)+'\n')
#         for image in images:
#             for layer in image['layers']:
#                 #print layer
#                 if layer['cache_id'] == k:
#                     layer['repeats'] = v
#                 #print layer
#
#
# def plt_repeat_layer(images):
#     d = {}
#     for image in images:
#         for layer in image['layers']:
#             if layer['repeats'] not in d:
#                 d[layer['repeats']] = []
#             d[layer['repeats']].append(layer['size'])
#             #print d
#
#     sort_layersbyrepeats = sorted(d.items())
#
#     x = []
#     y = []
#     for item in sort_layersbyrepeats:
#         print (item[0], item[1])
#         k = item[0]
#         v = item[1]
#         sum1 = sum(map(int, v))
#         x.append(int(k))
#         y.append(float(sum1)/len(v) / 1024 / 1024)
#
#     fig = fig_size('small')
#     plot_bar_pic(fig, x, y, 'Repeats', 'Average Size(MB)', max(x), max(x))
#
#     x = []
#     y = []
#     for item in sort_layersbyrepeats:
#         print (item[0], item[1])
#         k = item[0]
#         v = item[1]
#         sum1 = sum(map(int, v))
#         x.append(int(k))
#         y.append(float(sum1) / 1024 / 1024 / 1024)
#     fig = fig_size('small')
#     plot_bar_pic(fig, x, y, 'Repeats', 'Total Size(GB)', max(x), max(x))
#
#     x = []
#     y = []
#     for item in sort_layersbyrepeats:
#         print (item[0], item[1])
#         k = item[0]
#         v = item[1]
#         # sum1 = sum(map(int, v))
#         x.append(int(k))
#         y.append(len(v))
#     fig = fig_size('small')
#     plot_bar_pic(fig, x, y, 'Repeats', 'file count', max(x), max(x))

#def plt_repeat_layer(images):


# def chainID(diffs):
#     """ calculate chain-id from diff layers this is the same logical as in docker's source code"""
#     if len(diffs) == 0:
#         return ''
#
#     def chain_hash(x, y):
#         s = hashlib.sha256(bytearray(x + ' ' + y, 'utf8')).hexdigest()
#         return 'sha256:' + s
#     #print reduce(chain_hash, diffs)[7:]
#     return reduce(chain_hash, diffs)[7:]
#
# def load_images():
#     """load all images from /var/lib/docker/image/aufs/imagedb/content/sha256
#     files under this folder is the image spec json file, the filename is same as image id
#     """
#     images = []
#
#     content_dir = os.path.join(IMAGE_STORE_DIR, 'content/sha256')
#     for content_file in os.listdir(content_dir):
#         logging.info('found image: %s' % content_file)
#         with open(os.path.join(IMAGE_STORE_DIR, 'content/sha256', content_file)) as content:
#             image_content = json.load(content)
#
#         if 'rootfs' in image_content and 'diff_ids' in image_content['rootfs']:
#             diffs = image_content['rootfs']['diff_ids']
#             if isinstance(diffs, list) and len(diffs) > 0:
#                 chain_id = chainID(diffs)
#                 logging.debug('calcuated chain id from diff ids: %s' % chain_id)
#
#                 layers = []
#
#                 while chain_id:
#                     chain_id = get_chain_ids(chain_id)
#                     if chain_id:
#                         layers.append(load_layer(chain_id))
#                         print chain_id
#
#                 image = {
#                     'content_id': content_file,
#                     #'chain_id': chain_dis,
#                     'layers': layers,
#                     'layer_cnt': len(layers)
#                 }
#                 #print image
#                 images.append(image)
#             else:
#                 logging.warn('image content diffs is empty or invalid, skip this image')
#         else:
#             logging.warn('could not find rootfs diffs in image content, skip this image')
#     return images
#
# def get_chain_ids(chain_id):
#     parent_chainid_file = os.path.join(LAYER_STORE_DIR, 'sha256', chain_id, 'parent')
#     print  parent_chainid_file
#     if not os.path.isfile(parent_chainid_file):
#         logging.info('no parent_chainid_file file found for this chain id: %s', chain_id)
#         return None
#
#     with open(parent_chainid_file) as data_file:
#         temp = data_file.read()
#         parent_chain_id = temp.replace("sha256:", "")
#
#     if len(parent_chain_id) == 0:
#         logging.info('cache-id file is empty, no layers for this chain id:%s' % chain_id)
#         return None
#
#     logging.debug('found parent_chain_id for chain id %s -> %s' % (chain_id, parent_chain_id))
#     return parent_chain_id




















