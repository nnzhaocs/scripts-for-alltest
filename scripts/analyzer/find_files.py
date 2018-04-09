
from config import *
#from dir.py import *
# from load_files import *
# from config import *
from dir import *
from layers import check_file_type

""" TODO
Multi-processing

"""

def find_files():
    manager = multiprocessing.Manager()
    layer_dict = manager.dict()
    extracting_dir_size = manager.Value('i', 50*1000*1000*1000)

    if not construct_layer_info_or_not:
        construct_layer_info()
    else:
        with open(layer_dict_fname) as f:
            tmp_dict = json.load(f)
        for layer_id, layer_fname in tmp_dict.items():
            layer_dict[layer_id] = layer_fname

    layer_file_dict = find_layer_filename()
    # cnt = 0
    # layer_id, file_lst = zip(*layer_file_dict.items())
    # for layer_id, file_lst in layer_file_dict.items():
	# cnt = cnt + 1
	# try:
     #        layer_fname = layer_dict[layer_id]
     #    except:
	#     print("Cannot find layer_id in this machine's layer directories ################ ", layer_id)
	#     continue

        #"""decompress the layer to extracting dir"""
        # load_dirs(layer_fname, file_lst, output_dir, cnt)
        #"""get the files"""
    print "create pool"
    P = multiprocessing.Pool(num_worker_process)
    print "before map!"
    print len(layer_file_dict.items())  # process_manifest
    # print len(analyzed_layer_list)
    print "before map!"

    func = partial(load_dirs, layer_dict, extracting_dir_size)
    cnt = 0
    for i in layer_file_dict.items():
       if not i:
           continue
       if func(i):
           cnt = cnt + 1
           if cnt == 4:
                break
    #print layer_dict
    # P.map(func, layer_file_dict.items())
    print "after map"

    logging.info('done! all the layer job processes are finished')


def load_dirs(layer_dict, extracting_dir_size, dict_item):
    processname = multiprocessing.current_process().name
    layer_id, file_lst = dict_item
    logging.debug("[%s] process layer_filename: %s", processname, layer_id)
    #print layer_dict
    #return False
    ret = False

    try:
        layer_absfilename = layer_dict[layer_id]
	#print layer_absfilename
    except:
	#pass
        print("Cannot find layer_id in this machine's layer directories ################ ", layer_id)
	return ret
    #print layer_absfilename
    #return False
    filetype = check_file_type(layer_absfilename)

    layer_filename = os.path.basename(layer_absfilename)
    """ load the layer file in layer file dest_dir['layer_dir']/<layer_id>
    extracting to extracting_dir/<layer_id>
    load all the subdirs in this layer-id dir
    Then clean the layer-id dir """

    layer_file = layer_absfilename #os.path.join(dest_dir[0]['layer_dir'], layer_filename)

    extracting_dir = dest_dir[0]['extracting_dir']
    layer_dir = os.path.join(extracting_dir, layer_filename)
    mk_dir(layer_dir)

    output_dir_layer_dir = os.path.join(output_dir, layer_filename)
    #print output_dir_layer_dir
    #return ret
    cp_layer_tarball_name = os.path.join(extracting_dir, layer_filename + '-cp.tar.gzip')

    """
    extracting_dir:
                    layer_dir: layer_filename
                    cp_layer_tarball_name: layer_filename-cp.tar.zip
                    abs_zip_file_name: layer_filename-uncompressed-archival.tar
    """

    logging.debug('cp tarball to ==========> %s', layer_dir)

    sum_size = 0

    layer_compressed_size = os.lstat(layer_file).st_size

    while (extracting_dir_size.value - layer_compressed_size) <= 0:
        print("NO space left: %d", extracting_dir_size.value)

    if not cp_file(layer_file, cp_layer_tarball_name):
        clear_dir(layer_dir)
        return ret
    
    logging.debug('SPACE LEFT ==========> %d: sum_size:%d', extracting_dir_size.value/1024/1024/1024, sum_size/1024/1024/1024)

    extracting_dir_size.value = extracting_dir_size.value - layer_compressed_size
    sum_size = sum_size + layer_compressed_size

    logging.debug('SPACE LEFT ==========> %d: sum_size:%d', extracting_dir_size.value/1024/1024/1024, sum_size/1024/1024/1024)


    if not os.path.isfile(cp_layer_tarball_name):
        logging.warn('cp layer tarball file %s is invalid', cp_layer_tarball_name)
        clear_dir(layer_dir)
        extracting_dir_size.value = extracting_dir_size.value + sum_size
        return ret

    if filetype == 'gzip':
        logging.debug('STAT Extracting gzip file ==========> %s' % layer_file)
        abs_zip_file_name = os.path.join(extracting_dir, layer_filename + '-uncompressed-archival.tar')

        while extracting_dir_size.value - layer_compressed_size * 4 <= 0: #### make a guess
            print("NO space left: %d", extracting_dir_size.value)

        if not decompress_tarball_gunzip(cp_layer_tarball_name, abs_zip_file_name): # gunzip decompression
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            return ret

        size = os.lstat(abs_zip_file_name).st_size

        extracting_dir_size.value = extracting_dir_size.value - size
        sum_size = sum_size + size

        if os.path.isfile(abs_zip_file_name):
            uncompressed_archival_size = os.lstat(abs_zip_file_name).st_size
            logging.debug("uncompressed_archival_size %d B, name: %s", uncompressed_archival_size, layer_file)
        else:
            logging.debug("uncompressed_archival_wrong!!! name: %s", layer_file)
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            return ret

        while extracting_dir_size.value - size <= 0: #### archival == unpacking
            print("NO space left: %d", extracting_dir_size.value)

        if not extract_tarball(abs_zip_file_name, layer_dir):
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            return ret

        extracting_dir_size.value = extracting_dir_size.value - size
        sum_size = sum_size + size

        logging.debug('SPACE LEFT ==========> %d: sum_size:%d', extracting_dir_size.value / 1024 / 1024 / 1024,
                      sum_size / 1024 / 1024 / 1024)

        if not os.path.isdir(layer_dir):
            logging.warn('layer dir %s is invalid', layer_dir)
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            return ret

        if not remove_file(cp_layer_tarball_name):
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            return ret

        extracting_dir_size.value = extracting_dir_size.value + layer_compressed_size
        sum_size = sum_size - layer_compressed_size

        logging.debug('SPACE LEFT ==========> %d: sum_size:%d', extracting_dir_size.value / 1024 / 1024 / 1024,
                      sum_size / 1024 / 1024 / 1024)

    elif filetype == 'tar':
        logging.debug('STAT Extracting tar file ==========> %s' % layer_file)

        while extracting_dir_size.value - layer_compressed_size <= 0: #### archival == unpacking
            print("NO space left: %d", extracting_dir_size.value)

        if not extract_tarball(cp_layer_tarball_name, layer_dir):
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            return ret

        extracting_dir_size.value = extracting_dir_size.value - layer_compressed_size
        sum_size = sum_size + layer_compressed_size

        logging.debug('SPACE LEFT ==========> %d: sum_size:%d', extracting_dir_size.value / 1024 / 1024 / 1024,
                      sum_size / 1024 / 1024 / 1024)

        if not os.path.isdir(layer_dir):
            logging.warn('layer dir %s is invalid', layer_dir)
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            return ret

        if not remove_file(cp_layer_tarball_name):
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            return ret

        extracting_dir_size.value = extracting_dir_size.value + layer_compressed_size
        sum_size = sum_size + layer_compressed_size

        logging.debug('SPACE LEFT ==========> %d: sum_size:%d', extracting_dir_size.value / 1024 / 1024 / 1024,
                      sum_size / 1024 / 1024 / 1024)

    ret = load_files(layer_dir, file_lst, output_dir_layer_dir)
    #if not ret:
    clear_dir(layer_dir)

    extracting_dir_size.value = extracting_dir_size.value + sum_size
    sum_size = 0

    logging.debug('SPACE LEFT ==========> %d: sum_size:%d', extracting_dir_size.value/1024/1024/1024, sum_size/1024/1024/1024)
    return ret


def mv_files(src_absfname, des_dir):
    cmd = 'mv %s  %s' % (src_absfname, des_dir)
    logging.debug('The shell command: %s', cmd)
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.debug('###################%s: %s###################',
                      src_absfname, e.output)
        return False
    return True


def load_files(layer_dir, file_lst, output_dir):
    start = time.time()

    for fname in file_lst:
        file_name = fname.split('/',1)[1]
        dirname = os.path.dirname(fname)
        dir_name = dirname.split('/',1)[1]
        if os.path.isfile(os.path.join(layer_dir, file_name)):
            file_dirname = os.path.join(output_dir, dir_name)
            if not os.path.isdir(file_dirname):
                mk_dir(file_dirname)

            mv_files(os.path.join(layer_dir, file_name), file_dirname)

    elapsed = time.time() - start
    logging.info('process layer_id:%s : file move for layer, ==> %f s', layer_dir, elapsed)

    return True


def construct_layer_info():
    # layer_dict = {}
    for dir in layer_dirs:
        for path, subdirs, files in os.walk(dir):
            for f in files:
                layer_filename = os.path.join(os.path.join(path, f))
                layer_id = 'sha256:' + layer_filename.split("-")[1]
                layer_dict[layer_id] = layer_filename

    with open(layer_dict_fname, 'w+') as f_out:
        json.dump(layer_dict, f_out)

    return layer_dict


def find_layer_filename():
    layer_file_ldict = defaultdict(list)
    with open(find_file_lst_absfilename) as f:
        for line in f:
            line = line.rstrip('\n')
            layer_id = line.split(',', 1)[0] # split by \t ???????
            #layer_filename = layer_dict[layer_id]
            filename = line.split(',', 1)[1] # split by \t ???????

            layer_file_ldict[layer_id].append(filename)

    return layer_file_ldict
