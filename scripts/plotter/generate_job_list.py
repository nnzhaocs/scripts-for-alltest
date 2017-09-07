from draw_pic import *

def run_generatejoblist():
    logging.info('=============> run_generatejoblist <===========')

    bad_image_mappers = load_bad_image_mappers()
    list_50mb = load_job_list('list_less_50m.out')
    list_1gb = load_job_list('list_less_1g.out')
    list_2gb = load_job_list('list_less_2g.out')
    list_b_2gb = load_job_list('list_bigger_2g.out')

    config = []
    _digests = []

    layer_list_50mb = {}
    layer_list_1gb = {}
    layer_list_2gb = {}
    layer_list_b_2gb = {}

    for image_mapper in bad_image_mappers:
        config.append(image_mapper['non_downloaded_config_digest'])
        _digests.append(image_mapper['non_analyzed_layer_tarballs_digests'])

    digests = list(chain(*_digests))

    """check the size"""
    for digest in digests:
        try:
            val = list_50mb[digest]
        except:
            pass
        else:
            layer_list_50mb.update(val)

        try:
            val = list_1gb[digest]
        except:
            pass
        else:
            layer_list_1gb.update(val)

        try:
            val = list_2gb[digest]
        except:
            pass
        else:
            layer_list_2gb.update(val)

        try:
            val = list_b_2gb[digest]
        except:
            pass
        else:
            layer_list_b_2gb.update(val)

    with open(os.path.join(dest_dir[0]['job_list_dir'], 'layer_list_less_50m.out'), 'w+') as f_out:
        json.dump(layer_list_50mb, f_out)

    with open(os.path.join(dest_dir[0]['job_list_dir'], 'layer_list_less_1g.out'), 'w+') as f_out:
        json.dump(layer_list_1gb, f_out)

    with open(os.path.join(dest_dir[0]['job_list_dir'], 'layer_list_less_2g.out'), 'w+') as f_out:
        json.dump(layer_list_2gb, f_out)

    with open(os.path.join(dest_dir[0]['job_list_dir'], 'layer_list_bigger_2g.out'), 'w+') as f_out:
        json.dump(layer_list_b_2gb, f_out)


def extract_layer_config_name(filename):
    sstr = filename.split("-")
    name = "sha256:"+sstr[1]
    print "layer or config: name: %s, abs_filename: %s"%(name, filename)
    return name


def load_job_list(filename):
    job_dict = {}
    with open(os.path.join(dest_dir[0]['job_list_dir'], filename), 'r') as f:
        json_data = json.load(f)

    for key, val in json_data:
        tmp_dict = {}
        tmp_dict[key] = val
        digest = extract_layer_config_name(key)
        job_dict[digest] = tmp_dict

    return job_dict


def load_bad_image_mappers():
    """load_image_mappers"""
    # bad_image_mapper = {
    #     'version': version,
    #     'bad_manifest': manifest_name_dir_map,
    #     'non_downloaded_config': non_downloaded_config,
    #     'non_analyzed_layer_tarballs': non_analyzed_layer_tarballs
    # }

    bad_image_mappers = []

    with open(os.path.join(dest_dir[0]['job_list_dir'], 'bad_image_mapper.json'), 'r') as f:
        _image_mappers = json.load(f)

    for _image_mapper in _image_mappers:

        non_analyzed_layer_tarballs = []

        for key, val in _image_mapper['bad_manifest'].items():
            manifest_name = key

        if _image_mapper['non_downloaded_config']:
            for key, val in _image_mapper['non_downloaded_config'].items():
                non_downloaded_config_digest = val

        if _image_mapper['non_analyzed_layer_tarballs']:
            for key, val in _image_mapper['non_analyzed_layer_tarballs'].items():
                non_analyzed_layer_tarballs.append(key)

        bad_image_mapper = {
            'bad_manifest': manifest_name,
            'non_downloaded_config_digest': non_downloaded_config_digest,
            'non_analyzed_layer_tarballs_digests': non_analyzed_layer_tarballs
        }

        bad_image_mappers.append(bad_image_mapper)

    return bad_image_mappers



