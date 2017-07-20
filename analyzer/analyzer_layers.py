
from imports import *
from utility import *
from draw_pic import *


def layer_distribution(args):  # analyzed_layer_filename, layer_list_filename
    """create layer database as a json file"""
    logging.info('=============> create_layer_db: create layer metadata json file <===========')

    queue_layer_jsons()

    total = len(q_dir_layers)
    job_size = total / num_worker_process
    slices = chunks(q_dir_layers, job_size)
    jobs = []

    queue_list = []

    # print slices
    print len(slices)

    for i, s in enumerate(slices):
        job_queue = Queue.Queue()
        for item in s:
            job_queue.put(item)

        q_analyzed_layers = Queue.Queue()
        t = multiprocessing.Process(target=process_job, args=(i, job_queue, q_analyzed_layers))
        t.start()
        jobs.append(t)
        queue_list.append(q_analyzed_layers)

    for t in jobs:
        t.join()
    logging.info('done! all the layer job processes are finished')

    distribution_plot(queue_list)


def process_job(job_id, job_queue, q_analyzed_layers):
    logging.info('==============> starting layer_distribution %d <============', job_id)

    # q_flush_analyzed_layers = Queue.Queue()
    # q_flush_bad_unopen_layers = Queue.Queue()

    # lock_f_bad_unopen_layer = threading.Lock()
    # lock_f_analyzed_layer = threading.Lock()

    threads = []
    # flush_threads = []

    for i in range(num_worker_threads):
        t = threading.Thread(target=load_layer_json,
                             args=(job_queue, q_analyzed_layers))
        t.start()
        threads.append(t)

    # for j in range(num_flush_threads):
    #     t1 = threading.Thread(target=flush_file,
    #                           args=(f_analyzed_layer, q_flush_analyzed_layers, lock_f_analyzed_layer))
    #     t2 = threading.Thread(target=flush_file,
    #                           args=(f_bad_unopen_layer, q_flush_bad_unopen_layers, lock_f_bad_unopen_layer))
    #     t1.start()
    #     t2.start()
    #     flush_threads.append(t1)
    #     flush_threads.append(t2)

    job_queue.join()
    logging.info('wait job queue to join!')
    for i in range(num_worker_threads):
        job_queue.put(None)
    logging.info('put none layer json files to job queue!')
    for t in threads:
        t.join()
    logging.info('done! all the layer json file threads are finished')

    # q_flush_analyzed_layers.join()
    # q_flush_bad_unopen_layers.join()
    #
    # print "flush queues wait here!"
    # for i in range(num_flush_threads):
    #     q_flush_analyzed_layers.put(None)
    #     q_flush_bad_unopen_layers.put(None)
    # for t in flush_threads:
    #     t.join()
    # logging.info('done! all the flush threads are finished')


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
            q_dir_layers.append(layer_json_filename)
            logging.debug('queue dir layer json file: %s', layer_json_filename)  # str(layer_id).replace("/", "")
            # if not os.path.isfile(os.path.join(path, layer_json_filename)):
            #     logging.debug("layer json file %s is not valid!", layer_json_filename)
            #     continue


def load_layer_json(job_queue, q_analyzed_layers):
    layer_db_json_dir = dest_dir[0]['layer_db_json_dir']
    """load the layer dirs"""
    while True:
        layer_json_filename = job_queue.get()
        if layer_json_filename is None:
            logging.debug('The dir layer json queue is empty!')
            break

        if not os.path.isfile(os.path.join(layer_db_json_dir, layer_json_filename)):
            logging.debug("layer json file %s is not valid!", layer_json_filename)
            # q_flush_bad_unopen_layers.put(layer_json_filename)
            job_queue.task_done()
            continue

        logging.debug('process layer_json file: %s', layer_json_filename)  # str(layer_id).replace("/", "")

        # # layer_base_info = {}
        # uncompressed_sum_of_files = [] #  defaultdict(list)
        # compressed_size_with_method_gzip = []
        # archival_size = []
        # dir_max_depth = [] # defaultdict(list)
        # file_cnt = []

        with open(os.path.join(layer_db_json_dir, layer_json_filename)) as lj_f:
            try:
                json_data = json.load(lj_f)
            except:
                logging.debug("layer json file %s is not valid!", layer_json_filename)
                # q_flush_bad_unopen_layers.put(layer_json_filename)
            # i = i + 1
            # if i > 50:
            #   break
            uncompressed_sum_of_files = (json_data['size']['uncompressed_sum_of_files'] / 1024 / 1024)
            compressed_size_with_method_gzip = (
                json_data['size']['compressed_size_with_method_gzip'] / 1024 / 1024)
            archival_size = (json_data['size']['archival_size'] / 1024 / 1024)

            dir_max_depth = (json_data['layer_depth']['dir_max_depth'])

            file_cnt = (json_data['file_cnt'])

        # layer_base_info['uncompressed_sum_of_files'] = uncompressed_sum_of_files
        # layer_base_info['compressed_size_with_method_gzip'] = compressed_size_with_method_gzip
        # layer_base_info['archival_size'] = archival_size
        #
        # layer_base_info['dir_max_depth'] = dir_max_depth
        # layer_base_info['file_cnt'] = file_cnt
        layer_base_info = tuple(uncompressed_sum_of_files, compressed_size_with_method_gzip, archival_size, dir_max_depth, file_cnt)

        logging.debug("Put dict: %s to queues!", layer_base_info)

        q_analyzed_layers.put(layer_base_info)
        # q_flush_analyzed_layers.put(layer_base_info)
        job_queue.task_done()


def distribution_plot(queue_list):
    # f_bad_unopen_layer = open("bad_unopen_layer_jsonfile-%d.out" % job_id, 'a+')

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
    #             size['uncompressed_sum_of_files'].append(json_data['size']['uncompressed_sum_of_files'] / 1024 / 1024)
    #             size['compressed_size_with_method_gzip'].append(json_data['size']['compressed_size_with_method_gzip'] / 1024 / 1024)
    #             size['archival_size'].append(json_data['size']['archival_size'] / 1024 / 1024)
    #
    #             dir_depth['dir_max_depth'].append(json_data['layer_depth']['dir_max_depth'])
    #
    #             file_cnt.append(json_data['file_cnt'])
    # uncompressed_sum_of_files, compressed_size_with_method_gzip, archival_size, dir_max_depth, file_cnt
    if not len(queue_list):
        logging.debug("#####################queue list is empty!##################")

    for q_analyzed_layers in queue_list:
        while not q_analyzed_layers.empty():
            json_data = q_analyzed_layers.get()
            size['uncompressed_sum_of_files'].append(json_data[0])
            size['compressed_size_with_method_gzip'].append(json_data[1])
            size['archival_size'].append(json_data[2])

            dir_depth['dir_max_depth'].append(json_data[3])

            file_cnt.append(json_data[4])

    layer_base_info['size'] = size
    layer_base_info['dir_depth'] = dir_depth
    layer_base_info['file_cnt'] = file_cnt

    with open("analyzed_layer_jsonfile.out", 'a+') as f_analyzed_layer:
        json.dump(layer_base_info, f_analyzed_layer)

    fig = fig_size('small')  # 'large'

    data1 = layer_base_info['size']['uncompressed_sum_of_files']
    xlabel = 'layer size (MB)'
    xlim = max(data1)
    ticks = 25
    print xlim
    plot_cdf(fig, data1, xlabel, xlim, ticks)
