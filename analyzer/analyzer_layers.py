
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
            'compressed_size_with_mejson_datathod_gzip': compressed_size_with_method_gzip,
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


def layer_distribution(args):
    logging.info('==============> starting layer_distribution <============')
    queue_layer_jsons(args.layer_list_file)
    f_analyzed_layer = open("analyzed_layer_jsonfile.out", 'a+')
    f_bad_unopen_layer = open("bad_unopen_layer_jsonfile.out", 'a+')

    q_flush_analyzed_layers = Queue.Queue()
    q_flush_bad_unopen_layers = Queue.Queue()
    # q_bad_unopen_layers = Queue.Queue()

    lock_f_bad_unopen_layer = threading.Lock()
    lock_f_analyzed_layer = threading.Lock()
    q_analyzed_layers = Queue.Queue()
    # lock_q_analyzed_layer = Queue.Lock()
    threads = []
    flush_threads = []

    for i in range(num_worker_threads):
        t = threading.Thread(target=load_layer_json,
                             args=(q_analyzed_layers, q_flush_analyzed_layers, q_flush_bad_unopen_layers))
        t.start()
        threads.append(t)

    for j in range(num_flush_threads):
        t1 = threading.Thread(target=flush_file,
                              args=(f_analyzed_layer, q_flush_analyzed_layers, lock_f_analyzed_layer))
        t2 = threading.Thread(target=flush_file,
                              args=(f_bad_unopen_layer, q_flush_bad_unopen_layers, lock_f_bad_unopen_layer))
        t1.start()
        t2.start()
        flush_threads.append(t1)
        flush_threads.append(t2)

    q_dir_layers.join()
    logging.info('wait job queue to join!')
    for i in range(num_worker_threads):
        q_dir_layers.put(None)
    logging.info('put none layer json files to job queue!')
    for t in threads:
        t.join()
    logging.info('done! all the layer json file threads are finished')

    q_flush_analyzed_layers.join()
    q_flush_bad_unopen_layers.join()

    print "flush queues wait here!"
    for i in range(num_flush_threads):
        q_flush_analyzed_layers.put(None)
        q_flush_bad_unopen_layers.put(None)
    for t in flush_threads:
        t.join()
    logging.info('done! all the flush threads are finished')

    distribution_plot(q_flush_analyzed_layers)


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


def queue_layer_jsons():
    layer_dir = dest_dir[0]['layer_db_json_dir']
    for path, _, layer_json_filenames in os.walk(layer_dir):
        for layer_json_filename in layer_json_filenames:
            # logging.debug('layer_id: %s', layer_json_filename)  # str(layer_id).replace("/", "")
            q_dir_layers.put(layer_json_filename)
            logging.debug('queue dir layer json file: %s', layer_json_filename)  # str(layer_id).replace("/", "")
            # if not os.path.isfile(os.path.join(path, layer_json_filename)):
            #     logging.debug("layer json file %s is not valid!", layer_json_filename)
            #     continue


def load_layer_json(q_analyzed_layers, q_flush_analyzed_layers, q_flush_bad_unopen_layers):
    layer_db_json_dir = dest_dir[0]['layer_db_json_dir']
    """load the layer dirs"""
    while True:
        layer_json_filename = q_dir_layers.get()
        if layer_json_filename is None:
            logging.debug('The dir layer json queue is empty!')
            break

        if not os.path.isfile(os.path.join(layer_db_json_dir, layer_json_filename)):
            logging.debug("layer json file %s is not valid!", layer_json_filename)
            q_flush_bad_unopen_layers.put(layer_json_filename)
            q_dir_layers.task_done()
            continue

        logging.debug('process layer_json file: %s', layer_json_filename)  # str(layer_id).replace("/", "")

        layer_base_info = {}
        size = {} #  defaultdict(list)
        dir_depth = {} # defaultdict(list)
        file_cnt = []

        with open(os.path.join(layer_db_json_dir, layer_json_filename)) as lj_f:
            try:
                json_data = json.load(lj_f)
            except:
                logging.debug("layer json file %s is not valid!", layer_json_filename)
                q_flush_bad_unopen_layers.put(layer_json_filename)
            # i = i + 1
            # if i > 50:
            #   break
            size['uncompressed_sum_of_files'] = (json_data['size']['uncompressed_sum_of_files'] / 1024 / 1024)
            size['compressed_size_with_method_gzip'] = (
                json_data['size']['compressed_size_with_method_gzip'] / 1024 / 1024)
            size['archival_size'] = (json_data['size']['archival_size'] / 1024 / 1024)

            dir_depth['dir_max_depth'] = (json_data['layer_depth']['dir_max_depth'])

            file_cnt = (json_data['file_cnt'])

        layer_base_info['size'] = size
        layer_base_info['dir_depth'] = dir_depth
        layer_base_info['file_cnt'] = file_cnt

        # logging.info('sha256:' + layer_json_filename.split("-")[1])
        # # with lock_q_analyzed_layer:
        # if ('sha256:' + layer_filename.split("-")[1]) in q_analyzed_layers.queue:
        #     print "Layer Already Analyzed!"
        #     is_layer_analyzed = True
        # else:
        #     is_layer_analyzed = False
        #     print "Layer Not Analyzed!"
        #
        # if is_layer_analyzed:
        #     q_dir_layers.task_done()
        #     continue

        # compressed_size_with_method_gzip = os.lstat(os.path.join(dest_dir[0]['layer_dir'], layer_filename)).st_size
        # logging.debug("compressed_size_with_method_gzip %d B, name: %s", compressed_size_with_method_gzip,
        #               layer_filename)

        # start = time.time()
        # sub_dirs, archival_size = load_dirs(layer_filename)
        # elapsed = time.time() - start
        # logging.info('process directory: decompression plus sha digest calculation, consumed time ==> %f ; %d', elapsed,
        #              compressed_size_with_method_gzip)
        # if not len(sub_dirs):
        #     q_dir_layers.task_done()
        #     q_flush_bad_unopen_layers.put(layer_filename)
        #     # archival_size = clear_dirs(layer_filename)
        #     logging.debug('################### The layer dir wrong! layer file name %s ###################',
        #                   layer_filename)
        #     continue
        #
        # # archival_size = clear_dirs(layer_filename)
        # if archival_size == -1:
        #     q_dir_layers.task_done()
        #     return
        #
        # depths = [sub_dir['dir_depth'] for sub_dir in sub_dirs if sub_dir]
        # # if depths:
        # dir_depth = {
        #     'dir_max_depth': max(depths),
        #     'dir_min_depth': min(depths),
        #     'dir_median_depth': median(depths),
        #     'dir_avg_depth': mean(depths)
        # }
        # # print dir_depth
        # # else:
        # #     dir_depth = {
        # #         'dir_max_depth': 0,
        # #         'dir_min_depth': 0,
        # #         'dir_median_depth': 0,
        # #         'dir_avg_depth': 0
        # #     }
        #
        # sha, id, timestamp = str(layer_filename).split("-")
        #
        # size = {
        #     'uncompressed_sum_of_files': sum_layer_size(sub_dirs),
        #     'compressed_size_with_method_gzip': compressed_size_with_method_gzip,
        #     'archival_size': archival_size  # archival_size
        # }
        #
        # layer = {
        #     'layer_id': sha + ':' + id,  # str(layer_id).replace("/", ""),
        #     'dirs': sub_dirs,  # getLayersBychainID(chain_id),
        #     'layer_depth': dir_depth,
        #     'size': size,  # sum of files size,
        #     'repeats': 0,
        #     'file_cnt': sum_file_cnt(sub_dirs)
        # }
        #
        # # layer_q.put(layer)
        # abslayer_filename = os.path.join(layer_db_json_dir, layer_filename + '.json')
        # logging.info('write to layer json file: %s', abslayer_filename)
        # with open(abslayer_filename, 'w') as f_out:
        #     json.dump(layer, f_out)
        # # lock.acquire()
        # # layers.append(layer)
        # # lock.release()
        #
        # logging.debug('write layer_id:[%s]: to json file %s', layer_filename, abslayer_filename)
        q_analyzed_layers.put(layer_base_info)
        q_flush_analyzed_layers.put(layer_base_info)
        q_dir_layers.task_done()


def distribution_plot(q_flush_analyzed_layers):

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
    # #i = 0
    # layer_dir = dest_dir[0]['layer_db_json_dir']
    # for path, _, layer_json_filenames in os.walk(layer_dir):
    #     for layer_json_filename in layer_json_filenames:
    #         if not os.path.isfile(os.path.join(path, layer_json_filename)):
    #             logging.debug("layer json file %s is not valid!", layer_json_filename)
    #             continue
    #
    #         with open(os.path.join(path, layer_json_filename)) as lj_f:
    #             json_data = json.load(lj_f)
		# #i = i + 1
		# #if i > 50:
		#  #   break
    #             size['uncompressed_sum_of_files'].append(json_data['size']['uncompressed_sum_of_files'] / 1024 / 1024)
    #             size['compressed_size_with_method_gzip'].append(json_data['size']['compressed_size_with_method_gzip'] / 1024 / 1024)
    #             size['archival_size'].append(json_data['size']['archival_size'] / 1024 / 1024)
    #
    #             dir_depth['dir_max_depth'].append(json_data['layer_depth']['dir_max_depth'])
    #
    #             file_cnt.append(json_data['file_cnt'])
    while not q_analyzed_layers.empty():
        json_data = q_analyzed_layers.get()
        size['uncompressed_sum_of_files'].append(json_data['size']['uncompressed_sum_of_files'] / 1024 / 1024)
        size['compressed_size_with_method_gzip'].append(json_data['size']['compressed_size_with_method_gzip'] / 1024 / 1024)
        size['archival_size'].append(json_data['size']['archival_size'] / 1024 / 1024)

        dir_depth['dir_max_depth'].append(json_data['layer_depth']['dir_max_depth'])

        file_cnt.append(json_data['file_cnt'])

    layer_base_info['size'] = size
    layer_base_info['dir_depth'] = dir_depth
    layer_base_info['file_cnt'] = file_cnt

    fig = fig_size('small')  # 'large'

    data1 = layer_base_info['size']['uncompressed_sum_of_files']
    xlabel = 'layer size (MB)'
    xlim = max(data1)
    ticks = 25
    print xlim
    plot_cdf(fig, data1, xlabel, xlim, ticks)
