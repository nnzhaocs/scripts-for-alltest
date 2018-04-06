
from config import *
from decompression_methods import *

def test_decompression():
    layer_dict = load_layer_dict()
    layer_fnames = load_testing_layer_lst(layer_dict)

    print "create pool"
    P = multiprocessing.Pool(num_worker_process)
    print "before map!"
    print len(layer_fnames)  # process_manifest
    # print len(analyzed_layer_list)
    print "before map!"
    #for i in layer_job_list:
    #    if not i:
    #        continue
    #    process_layer(i)
        #break
    P.map(process_layer, layer_fnames)
    print "after map"

    logging.info('done! all the layer job processes are finished')


def load_testing_layer_lst(layer_dict):
    layer_fnames = []
    with open(testing_layer_lst_absfilename) as f:
        for line in f:
            line = line.rstrip('\n')
            try:
                layer_filename = layer_dict[line]
            except:
                print("Cannot find layer_id in this machine's layer directories ################ ", line)
                continue
            layer_fnames.append(layer_filename)

    return layer_fnames


def load_layer_dict():
    with open(layer_dict_fname) as f:
        layer_dict = json.load(f)

    return layer_dict


def process_layer(layer_filename):
    processname = 0 #multiprocessing.current_process().name
    logging.debug("[%s] process layer_filename: %s", processname, layer_filename)

    layer_db_json_dir = dest_dir[0]['layer_db_json_dir']

    if layer_filename is None:
        logging.debug('The layer queue is None!')
        return

    if "sha256-" not in layer_filename:
        logging.info('file %s is not a layer tarball or config file', layer_filename)
        return False
    if len(layer_filename.split("-")) != 3:
        logging.debug('The layer filename is invalid %s!', layer_filename)
        return False

    logging.info('sha256:' + layer_filename.split("-")[1])

    if ('sha256:' + layer_filename.split("-")[1]) in analyzed_layer_list:
        print "Layer Already Analyzed!"
        is_layer_analyzed = True
    else:
        is_layer_analyzed = False
        print "Layer Not Analyzed!"

    if is_layer_analyzed:
        return

    if not os.path.isfile(os.path.join(dest_dir[0]['layer_dir'], layer_filename)):
        logging.info('file %s is not valid', layer_filename)
        return False

    start = time.time()

    filetype = check_file_type(layer_filename)

    if filetype == 'tar':
        print "This is a tar file"
        archival_size = os.lstat(os.path.join(dest_dir[0]['layer_dir'], layer_filename)).st_size
        logging.debug("archival_size %d B, name: %s", archival_size, layer_filename)
        compressed_archival_size, -1 = load_dirs(layer_filename, filetype)
        elapsed = time.time() - start
        logging.info('process layer_id:%s : total time, consumed time ==> %f s; compress size: %d',
                     layer_filename, elapsed, compressed_archival_size)
    elif filetype == 'gzip':
        print "This is a gzip file"
        # compressed_archival_size = os.lstat(os.path.join(dest_dir[0]['layer_dir'], layer_filename)).st_size
        logging.debug("compressed_size_with_method_gzip %d B, name: %s", compressed_size_with_method_gzip, layer_filename)
        compressed_archival_size, archival_size = load_dirs(layer_filename, filetype)
        elapsed = time.time() - start
        logging.info('process layer_id:%s : total time, consumed time ==> %f s; compress size: %d',
                     layer_filename, elapsed, compressed_archival_size)
    else:
        logging.info('################### The layer tarball type is neither tar or gzip! layer file name %s ###################', layer_filename)
        return

    if not sub_dirs:
        """"write to bad layer_tarball"""
        with open("bad_nonanalyzed_layer_list-%s.out" % processname, 'a+') as f:
            f.write(layer_filename+'\n')
        logging.debug('################### The layer tarball has problems! layer file name %s ###################', layer_filename)
        return

    if archival_size == -1 or compressed_archival_size == -1:
        return

    # depths = [sub_dir['dir_depth'] for sub_dir in sub_dirs if sub_dir]
    # dir_depth = {
    #     'dir_max_depth': max(depths),
    #     'dir_min_depth': min(depths),
    #     'dir_median_depth': statistics.median(depths),
    #     'dir_avg_depth': statistics.mean(depths)
    # }

    # sha, id, timestamp = str(layer_filename).split("-")
    size = {
        # 'uncompressed_sum_of_files': sum_layer_size(sub_dirs),
        'compressed_size_with_method'+'_'+method: compressed_archival_size,
        'archival_size': archival_size   # archival_size
    }

    layer = {
        'layer_id': sha+':'+id,  # str(layer_id).replace("/", ""),
        # 'dirs': sub_dirs,  # getLayersBychainID(chain_id),
        # 'layer_depth': dir_depth,
        'size': size,  # sum of files size,
        # 'repeats': 0,
        # 'file_cnt': sum_file_cnt(sub_dirs)
    }

    abslayer_filename = os.path.join(layer_db_json_dir, layer_filename+'.json')
    logging.info('write to layer json file: %s', abslayer_filename)
    with open(abslayer_filename, 'w+') as f_out:
        json.dump(layer, f_out)

    logging.debug('write layer_id:[%s]: to json file %s', layer_filename, abslayer_filename)

    with open("analyzed_layer_filename-%s.out" % processname, 'a+') as f:
        f.write(layer_filename + '\n')


