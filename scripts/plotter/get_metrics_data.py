
# from imports import *
# from utility import *
from draw_pic import *

# q_analyzed_layer_jsons = []
# q_dir_layer_jsons = []

image_mappers = []
image_metrics_data = {}
"""image_metrics_data content

    version: schema1:
            schema2:
            schemalist:
    size: compressed
            sum of 
            archival
    compression ratio:
    repeat layer cnt:
    file cnt:
    layer depth:
"""

layer_metrics_data = {}
"""layer_metrics_data content
    size: compressed
            sum of 
            archival
    compression ratio:
    repeat file cnt:
    file cnt:
    layer depth:
    file size:
    file type:
    
"""

def run_getmetricsdata():
    logging.info('=============> run_getmetricsdata <===========')
    queue_layer_jsons()
    print "create pool"
    P = multiprocessing.Pool(60)
    print "before map"
    q_analyzed_layer_jsons = P.map(load_metrics_data, image_mappers)
    #P.close()
    #P.join()
    print "after map"
    for i in q_analyzed_layer_jsons:
	#while not q_analyzed_layer_json.empty():
        print i

    with open("saved_analyzed_layer_jsonfile.out", 'w+') as f_analyzed_layer:
        json.dump(q_analyzed_layer_jsons, f_analyzed_layer)
    #distribution_plot()


def load_metrics_data(image_mapper):
    # image_mapper = {
    #     'version': version,
    #     'manifest': manifest_name_dir_map{},
    #     'config': config_name_dir_map{},
    #     'layers': layers_map{:{:}}
    # }

    layer_db_json_dir = dest_dir[0]['layer_db_json_dir']
    layer_base_info = {}
    if layer_json_filename is None:
	# print 'The dir layer json queue is empty!'
        laogging.debug('The dir layer json queue is empty!')
        return None

    if not os.path.isfile(os.path.join(layer_db_json_dir, layer_json_filename)):
        logging.debug("layer json file %s is not valid!", layer_json_filename)
        return None

    logging.debug('process layer_json file: %s', layer_json_filename)  # str(layer_id).replace("/", "")

    # try:
    #    lj_f = open(os.path.join(layer_db_json_dir, layer_json_filename))
    ## except:
    #    logging.debug("cannot open layer json file, %s" % layer_json_filename)
    #    return None
    #    lj_f.close()

    with open(os.path.join(layer_db_json_dir, layer_json_filename)) as lj_f:
        try:
            json_data = json.load(lj_f)
        except:
	    # print "cannot load json file: layer json file %s is not valid!"
            logging.debug("cannot load json file: layer json file %s is not valid!", layer_json_filename)
            lj_f.close()
            return None

        uncompressed_sum_of_files = json_data['size']['uncompressed_sum_of_files']
        compressed_size_with_method_gzip = json_data['size']['compressed_size_with_method_gzip']
        archival_size = json_data['size']['archival_size']

        dir_max_depth = json_data['layer_depth']['dir_max_depth']

        file_cnt = json_data['file_cnt']

        del json_data

    #layer_base_info = [uncompressed_sum_of_files, compressed_size_with_method_gzip, archival_size, dir_max_depth, file_cnt]

    layer_base_info['uncompressed_sum_of_files'] = uncompressed_sum_of_files
    layer_base_info['compressed_size_with_method_gzip'] = compressed_size_with_method_gzip
    layer_base_info['archival_size'] = archival_size

    layer_base_info['dir_max_depth'] = dir_max_depth
    layer_base_info['file_cnt'] = file_cnt
    layer_base_info['sum_to_gzip_ratio'] = uncompressed_sum_of_files*1.0/compressed_size_with_method_gzip
    layer_base_info['archival_to_gzip_ratio'] = archival_size*1.0/compressed_size_with_method_gzip
    logging.debug("Put tuple: %s to queues!", layer_base_info)
    return layer_base_info
    #q_analyzed_layer_jsons.put(layer_base_info)


def load_image_mappers():
    """load_image_mappers"""
    # image_mapper = {
    #     'version': version,
    #     'manifest': manifest_name_dir_map{},
    #     'config': config_name_dir_map{},
    #     'layers': layers_map{:{:}}
    # }

    with open(os.path.join(dest_dir[0]['job_list_dir'],'image_mapper.json'), 'r') as f:
        _image_mappers = json.load(f)

    for _image_mapper in _image_mappers:

        manifest_name_dir_map = {}
        config_name_dir_map = {}
        layers_map = {}

        for key, val in _image_mapper['manifest'].items():
            manifest_name_dir_map[key] = val

        if _image_mapper['config']:
            for key, val in _image_mapper['config'].items():
                config_name_dir_map[key] = val

        if _image_mapper['layers']:
            layer_name_dir_map = {}
            for key, val in _image_mapper['layers'].items():
                for key1, val1 in val.items():
                    layer_name_dir_map[key1] = val1
                layers_map[key] = layer_name_dir_map

        image_mapper = {
            'version': _image_mapper['version'],
            'manifest': manifest_name_dir_map,
            'config': config_name_dir_map,
            'layers': layers_map,
            'has_non_analyzed_layer_tarballs': _image_mapper['has_non_analyzed_layer_tarballs'],
            'has_non_downloaded_config': _image_mapper['has_non_downloaded_config']
        }

        image_mappers.append(image_mapper)

        # for key, val in _manifest_map_dir.items():
        #     manifest_map_dir[key] = val
        # # manifest_names = list(manifest_map_dir.keys())
        # for i in manifest_map_dir.keys():
        #     manifest_names.append(i)
    # if bad_image_mapper:
    # with open(os.path.join(dest_dir[0]['job_list_dir'],'bad_image_mapper.json'), 'w') as f:
    #     json.dump(bad_image_mapper, f)

    # layer_dir = dest_dir[0]['layer_db_json_dir']
    # i = 0
    # for path, _, layer_json_filenames in os.walk(layer_dir):
    #     for layer_json_filename in layer_json_filenames:
    #         i = i + 1
    #         #if i > 50:
    #          #   break
    #         # logging.debug('layer_id: %s', layer_json_filename)  # str(layer_id).replace("/", "")
    #         q_dir_layer_jsons.append(layer_json_filename)
    #         logging.debug('queue dir layer json file: %s', layer_json_filename)  # str(layer_id).replace("/", "")


#def distribution_plot():
#    layer_base_info = {}
#    size = defaultdict(list)
#    dir_depth = defaultdict(list)
#    file_cnt = []
#
#    print "===============> start ploting <=============="
#
#    # for q_analyzed_layers in q_analyzed_layer_jsons:
#    while not q_analyzed_layer_jsons.empty():
#        json_data = q_analyzed_layer_jsons.get()
#        print json_data
#        #size['uncompressed_sum_of_files'].append(json_data[0])
#        #size['compressed_size_with_method_gzip'].append(json_data[1])
#        #size['archival_size'].append(json_data[2])
#
#        #dir_depth['dir_max_depth'].append(json_data[3])
#
#        #file_cnt.append(json_data[4])
#
#        size['uncompressed_sum_of_files'].append(json_data['uncompressed_sum_of_files'] / 1024 / 1024)
#        size['compressed_size_with_method_gzip'].append(json_data['compressed_size_with_method_gzip'] / 1024 / 1024)
#        size['archival_size'].append(json_data['archival_size'] / 1024 / 1024)
#
#        dir_depth['dir_max_depth'].append(json_data['dir_max_depth'])
#
#        file_cnt.append(json_data['file_cnt'])
#
#    layer_base_info['size'] = size
#    layer_base_info['dir_depth'] = dir_depth
#    layer_base_info['file_cnt'] = file_cnt
#
#    with open("saved_analyzed_layer_jsonfile.out", 'a+') as f_analyzed_layer:
#        json.dump(layer_base_info, f_analyzed_layer)
#
#    fig = fig_size('small')  # 'large'
#
#    data1 = layer_base_info['size']['uncompressed_sum_of_files']
#    xlabel = 'Uncompressed layer size (MB): sum of files'
#    xlim = mean(data1)  # max(data1)
#    ticks = 25
#    print xlim
#    plot_cdf(fig, data1, xlabel, xlim, ticks)

