
from imports import *
from draw_pic import *
from utility import *
from dir import *
from itertools import chain

"""TODO:
    1. check duplicated.
    2. mount tmpfs
    3. already has json file no need to extracting again
"""


def create_layer_db(analyzed_layer_filename, extracting_dir):
    """create layer database as a json file"""
    logging.info('=============> create_layer_db: create layer metadata json file <===========')

    queue_layers(analyzed_layer_filename)

    f_analyzed_layer = open(analyzed_layer_filename, 'a+')
    f_bad_unopen_layer = open("bad_unopen_layer_list.out", 'a+')

    for i in range(num_worker_threads):
        t = threading.Thread(target=load_layer, args=(extracting_dir, dest_dir[0]['layer_db_json_dir']))
        t.start()
        threads.append(t)

    for j in range(num_flush_threads):
        t1 = threading.Thread(target=flush_file, args=(f_analyzed_layer, q_flush_analyzed_layers, lock_f_analyzed_layer))
        t2 = threading.Thread(target=flush_file, args=(f_bad_unopen_layer, q_flush_bad_unopen_layers, lock_f_bad_unopen_layer))
        t1.start()
        t2.start()
        flush_threads.append(t1)
        flush_threads.append(t2)

    q_dir_layers.join()
    logging.info('wait queue to join!')
    for i in range(num_worker_threads):
        q_dir_layers.put(None)
    logging.info('put none layers to queue!')
    for t in threads:
        t.join()
    logging.info('done! all the layer threads are finished')

    q_flush_analyzed_layers.join()
    q_flush_bad_unopen_layers.join()

    print "flush queues wait here!"
    for i in range(num_flush_threads):
        q_flush_analyzed_layers.put(None)
        q_flush_bad_unopen_layers.put(None)
    for t in flush_threads:
        t.join()

    # while not layer_q.empty():
    #     layer = layer_q.get()
    #     # if layer is None:
    #     #     break
    #     layers.append(layer)
        # layer_q.task_done()
    # json.dump(layers, f_layer_db)
    # f_out.close()


def queue_layers(analyzed_layer_filename):
    """queue the layer id in downloaded_layer_filename, layer id = sha256:digest !!! without timestamp"""
    # with open(downloaded_layer_filename) as f:
    #     for line in f:
    #         print line
    #         if line:
    #             logging.debug('queue layer_id: %s to downloaded layer queue', line.replace("\n", ""))  #
    #             q_downloaded_layers.put(line.replace("\n", ""))
    """queue the layer id in analyzed_layer_filename, layer id = sha256:digest !!! without timestamp"""
    with open(analyzed_layer_filename) as f:
        for line in f:
            print line
            if line:
                logging.debug('queue layer_id: %s to analyzed_layer_queue', line.replace("\n", ""))  #
                q_analyzed_layers.put(line.replace("\n", ""))

    """queue the layer id in dest_dir/layers, layer id = sha256-digest-timestamp"""
    for path, _, tarball_filenames in os.walk(dest_dir[0]['layer_dir']):
        for tarball_filename in tarball_filenames:
            logging.debug('layer_id: %s', tarball_filename)  # str(layer_id).replace("/", "")
            q_dir_layers.put(tarball_filename)
            logging.debug('queue dir layer tarball: %s', tarball_filename)  # str(layer_id).replace("/", "")


def check_config_file(filename):
    tarball_filename = os.path.join(dest_dir[0]['layer_dir'], filename)
    cmd2 = 'file %s' % tarball_filename
    logging.debug('The shell command: %s', cmd2)

    proc = subprocess.Popen(cmd2, stdout=subprocess.PIPE, shell=True)
    out, err = proc.communicate()
    logging.debug('The shell output: %s', out)
    if 'gzip' in out:
        return False
    else:
        return True


def move_config_file(filename):
    config_filename = os.path.join(dest_dir[0]['layer_dir'], filename)
    cmd3 = 'mv %s %s' % (config_filename, dest_dir[0]['config_dir'])
    logging.debug('The shell command: %s', cmd3)
    # rc = os.system(cmd3)
    # assert (rc == 0) # or use try exception
    # # logging.debug('The shell output: %s', out)
    try:
        subprocess.check_output(cmd3, shell=True)
    except subprocess.CalledProcessError as e:
        print e.output


def is_valid_tarball(layer_filename):
    if "sha256-" not in layer_filename:
        logging.info('file %s is not a layer tarball or config file', layer_filename)
        return False
    if len(layer_filename.split("-")) != 3:
        logging.debug('The layer filename is invalid %s!', layer_filename)
        return False
    if not os.path.isfile(os.path.join(dest_dir[0]['layer_dir'], layer_filename)):
        logging.info('layer tarball file %s is not valid', layer_filename)
        return False
    if check_config_file(layer_filename):
        move_config_file(layer_filename)
        return False
    return True


def load_layer(extracting_dir, layer_db_json_dir):
    """load the layer dirs"""
    while True:
        layer_filename = q_dir_layers.get()
        if not is_valid_tarball(layer_filename):
            q_dir_layers.task_done()
            continue

        logging.debug('process layer_dir: %s', layer_filename)  # str(layer_id).replace("/", "")
        if layer_filename is None:
            logging.debug('The dir layer queue is empty!')
            break

        logging.info('sha256:' + layer_filename.split("-")[1])
        with lock_q_analyzed_layer:
            if ('sha256:' + layer_filename.split("-")[1]) in q_analyzed_layers.queue:
                print "Layer Already Analyzed!"
                is_layer_analyzed = True
            else:
                is_layer_analyzed = False
                q_analyzed_layers.put('sha256:' + layer_filename.split("-")[1])
                print "Layer Not Analyzed!"

        if is_layer_analyzed:
            q_dir_layers.task_done()
            continue

        sub_dirs = load_dirs(layer_filename, extracting_dir)
        if not len(sub_dirs):
            q_dir_layers.task_done()
            clear_dirs(layer_filename, extracting_dir)
            logging.debug('The dir wrong!')
            continue

        clear_dirs(layer_filename, extracting_dir)

        depths = [sub_dir['dir_depth'] for sub_dir in sub_dirs if sub_dir]
        if depths:
            dir_depth = max(depths)
            # print dir_depth
        else:
            dir_depth = 0

        sha, id, timestamp = str(layer_filename).split("-")

        layer = {
            'layer_id': sha+':'+id,  # str(layer_id).replace("/", ""),
            'dirs': sub_dirs,  # getLayersBychainID(chain_id),
            'dir_max_depth': dir_depth,
            'size': 0,  # getLayersSize(chainid),
            'repeats': 0
        }

        # layer_q.put(layer)
        abslayer_filename = os.path.join(layer_db_json_dir, layer_filename+'.json')
        logging.info('write to layer json file: %s', abslayer_filename)
        with open(abslayer_filename, 'w') as f_out:
            json.dump(layer, f_out)
        # lock.acquire()
        # layers.append(layer)
        # lock.release()

        logging.debug('write layer_id:[%s]: to json file %s', layer_filename, abslayer_filename)
        q_flush_analyzed_layers.put('sha256:' + layer_filename.split("-")[1])
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


def cal_layer_repeats(images):
    """ [imge1's layers, image2's layers, ..... ]
    get unique elements among multiple lists: all_list"""
    #fout = open('layer-repeats.txt', 'w+')
    layers = []
    for image in images:
        image_layers = []
        for layer in image['layers']:
            #print layer
            diff_path = os.path.join(dest_dir['layer_dir'], layer['cache_id'])
            logging.debug('%s', '\n'.join(diff_path))
            """here, find all layer for each image"""
            image_layers.append(layer['cache_id'])
        layers.append(image_layers)
    print layers[0]

    layer_union = list(chain(*layers))
    print layer_union[0]
    layer_dict = {i:layer_union.count(i) for i in layer_union}
    #layer_repeats_dict = cal_layer_repeats(images)
    for k, v in layer_dict.items():
        print (k, v)
        #fout.writelines(str(k)+','+str(v)+'\n')
        for image in images:
            for layer in image['layers']:
                #print layer
                if layer['cache_id'] == k:
                    layer['repeats'] = v
                #print layer


def plt_repeat_layer(images):
    d = {}
    for image in images:
        for layer in image['layers']:
            if layer['repeats'] not in d:
                d[layer['repeats']] = []
            d[layer['repeats']].append(layer['size'])
            #print d

    sort_layersbyrepeats = sorted(d.items())

    x = []
    y = []
    for item in sort_layersbyrepeats:
        print (item[0], item[1])
        k = item[0]
        v = item[1]
        sum1 = sum(map(int, v))
        x.append(int(k))
        y.append(float(sum1)/len(v) / 1024 / 1024)

    fig = fig_size('small')
    plot_bar_pic(fig, x, y, 'Repeats', 'Average Size(MB)', max(x), max(x))

    x = []
    y = []
    for item in sort_layersbyrepeats:
        print (item[0], item[1])
        k = item[0]
        v = item[1]
        sum1 = sum(map(int, v))
        x.append(int(k))
        y.append(float(sum1) / 1024 / 1024 / 1024)
    fig = fig_size('small')
    plot_bar_pic(fig, x, y, 'Repeats', 'Total Size(GB)', max(x), max(x))

    x = []
    y = []
    for item in sort_layersbyrepeats:
        print (item[0], item[1])
        k = item[0]
        v = item[1]
        # sum1 = sum(map(int, v))
        x.append(int(k))
        y.append(len(v))
    fig = fig_size('small')
    plot_bar_pic(fig, x, y, 'Repeats', 'file count', max(x), max(x))

#def plt_repeat_layer(images):
