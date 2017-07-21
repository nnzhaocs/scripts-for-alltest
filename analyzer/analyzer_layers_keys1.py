


from imports import *
from utility import *
from draw_pic import *

q_analyzed_layers = multiprocessing.Queue()
q_dir_layers = []

def load_layer_json(layer_json_filename):
    layer_db_json_dir = '/gpfs/docker_images_largefs/layer_db_json_bison03p'
    """load the layer dirs"""
    # while True:
        # layer_json_filename = job_queue.get()
    #layer_base_info = []
    #json_data = None
    if layer_json_filename is None:
        logging.debug('The dir layer json queue is empty!')
        return
        # break

    if not os.path.isfile(os.path.join(layer_db_json_dir, layer_json_filename)):
        logging.debug("layer json file %s is not valid!", layer_json_filename)
        return
        # q_flush_bad_unopen_layers.put(layer_json_filename)
        # job_queue.task_done()
        # continue

    logging.debug('process layer_json file: %s', layer_json_filename)  # str(layer_id).replace("/", "")

    # # layer_base_info = {}
    # uncompressed_sum_of_files = [] #  defaultdict(list)
    # compressed_size_with_method_gzip = []
    # archival_size = []
    # dir_max_depth = [] # defaultdict(list)
    # file_cnt = []A
    #uncompressed_sum_of_files = 12
    #compressed_size_with_method_gzip = 10
    #archival_size = 11
    #dir_max_depth = 12
    #file_cnt = 1
    # try:
    with open(os.path.join(layer_db_json_dir, layer_json_filename)) as lj_f:
    ## except:
    ##     logging.debug("cannot open layer json file %s!", layer_json_filename)
    ##     job_queue.task_done()
    ##     continue
        try:
            json_data = json.load(lj_f)
        except:
            logging.debug("layer json file %s is not valid!", layer_json_filename)
            lj_f.close()
            return
            # q_flush_bad_unopen_layers.put(layer_json_filename)

        uncompressed_sum_of_files = json_data['size']['uncompressed_sum_of_files'] / 1024 / 1024
        compressed_size_with_method_gzip = json_data['size']['compressed_size_with_method_gzip'] / 1024 / 1024
        archival_size = json_data['size']['archival_size'] / 1024 / 1024

        dir_max_depth = json_data['layer_depth']['dir_max_depth']

        file_cnt = json_data['file_cnt']
	# json_data = None
	del json_data

    #layer_base_info['uncompressed_sum_of_files'] = uncompressed_sum_of_files
    #layer_base_info['compressed_size_with_method_gzip'] = compressed_size_with_method_gzip
    #layer_base_info['archival_size'] = archival_size
    
    #layer_base_info['dir_max_depth'] = dir_max_depth
    #layer_base_info['file_cnt'] = file_cnt
    layer_base_info = [uncompressed_sum_of_files, compressed_size_with_method_gzip, archival_size, dir_max_depth, file_cnt]

    print("Put tuple: %s to queues!", layer_base_info)

    q_analyzed_layers.put(layer_base_info)
    #print "queue: %s" % q_analyzed_layers
    # # q_flush_analyzed_layers.put(layer_base_info)
    # job_queue.task_done()
    #del json_data
    layer_base_info=[]

def queue_layer_jsons():
    layer_dir = '/gpfs/docker_images_largefs/layer_db_json_bison03p'
    i = 0
    for path, _, layer_json_filenames in os.walk(layer_dir):
        for layer_json_filename in layer_json_filenames:
            i = i + 1
            #if i > 50:
             #   break
            # logging.debug('layer_id: %s', layer_json_filename)  # str(layer_id).replace("/", "")
            q_dir_layers.append(layer_json_filename)
            logging.debug('queue dir layer json file: %s', layer_json_filename)  # str(layer_id).replace("/", "")


if __name__ == '__main__':
    queue_layer_jsons()
    P = multiprocessing.Pool(60)
    print(P.map(load_layer_json, q_dir_layers))
    while not q_analyzed_layers.empty():
	print q_analyzed_layers.get()
