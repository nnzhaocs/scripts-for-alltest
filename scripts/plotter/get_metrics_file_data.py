
from algorithm_funcs import *

image_mappers = []
layer_mappers = []


file_metrics_datas = []
"""file_metrics_data content
    file size:
    file type:
    file extension:
    sha256:
    ctime
    mtime
    atime
    uid
    gid
"""


def run_getmetrics_file_data():
    logging.info('=============> run_getmetricsdata <===========')

    load_image_mappers()
    load_layers_mappers()

    print "create pool"
    P2 = multiprocessing.Pool(60)
    print "before map"

    layer_mappers_slices = [layer_mappers[i:i + 24] for i in range(0, len(layer_mappers), 24)]
    file_metrics_datas = P2.map(load_file_metrics_data, layer_mappers_slices)

    print "after map"

    #for layer_mapper in layer_mappers:
    #    load_file_metrics_data(layer_mapper)

    with open(os.path.join(dest_dir[0]['job_list_dir'], 'file_metrics_datas.json'), 'w+') as f_layer_metrics_datas:
        json.dump(file_metrics_datas, f_layer_metrics_datas)

    calaculate_file_metrics()

def load_file_metrics_data(_layer_mappers):

    """file stat_type file type file size"""
    file_metrics_data = defaultdict(list)

    for layer_mapper in _layer_mappers:
        for key, val in layer_mapper.items(): # only one entry
            logging.debug("key: %s, val: %s!", key, val)
            layer_json_absfilename = val

            if not os.path.isfile(layer_json_absfilename):
                logging.debug("layer json file %s is not valid!", layer_json_absfilename)
                continue

            logging.debug('process layer_json file: %s', layer_json_absfilename)  # str(layer_id).replace("/", "")
            with open(layer_json_absfilename) as lj_f:
                try:
                    json_data = json.load(lj_f)
                except:
                    logging.debug("cannot load json file: layer json file %s is not valid!", layer_json_absfilename)
                    lj_f.close()
                    continue

                for subdir in json_data['dirs']:
                    for sub_file in subdir['files']:
                        file_metrics_data['type'].append(sub_file['type'])
                        file_metrics_data['sha256'].append(sub_file['sha256'])

                        file_metrics_data['stat_type'].append(sub_file['file_info']['stat_type']) #['sha256']
                        file_metrics_data['stat_size'].append(sub_file['file_info']['stat_size'])

                del json_data

    logging.debug("layer_metrics_data: %s", file_metrics_data)
    return file_metrics_data


def calaculate_file_metrics():
    """get repeat files"""

    file_types = []
    file_sha256s = []
    file_stat_types = []

    _file_metrics_datas = list(chain(*file_metrics_datas))

    for file_metrics_data in _file_metrics_datas:
        file_types.append(file_metrics_data['type'])
        file_sha256s.append(file_metrics_data['sha256'])
        file_stat_types.append(file_metrics_data['stat_type'])

    file_sha256s_dict = calculate_repeates(file_sha256s)

    with open(os.path.join(dest_dir[0]['job_list_dir'], 'repeate_file_sha256s.json'), 'w') as f:
        json.dump(file_sha256s_dict, f)

    """file types"""

    file_types_dict = calculate_repeates(file_types)

    with open(os.path.join(dest_dir[0]['job_list_dir'], 'repeate_file_types_dict.json'), 'w') as f:
        json.dump(file_types_dict, f)

    """file stat_type"""

    file_stat_types_dict = calculate_repeates(file_stat_types)

    with open(os.path.join(dest_dir[0]['job_list_dir'], 'repeate_file_stat_types_dict.json'), 'w') as f:
        json.dump(file_stat_types_dict, f)


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

    logging.debug("load image_mapper: %s", os.path.join(dest_dir[0]['job_list_dir'],'image_mapper.json'))

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
    
    logging.debug("image_mappers[0]: %s", image_mappers[0])


def load_layers_mappers():
    layer_mapper = {}
    for image_mapper in image_mappers:
        for key, val in image_mapper['layers'].items():
            for key1, val1 in val.items(): # only one entry
                layer_json_absfilename = val1 #json_absfilename
                layer_mapper[key] = layer_json_absfilename

    for key, val in layer_mapper.items():
        tmp_mapper = {}
        tmp_mapper[key] = val
        layer_mappers.append(tmp_mapper)



