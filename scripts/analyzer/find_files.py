
from config import *
#from dir.py import *
# from load_files import *
# from config import *
from dir import *
from layers import check_file_type

""" 
TODO
Multi-processing

"""
#import operator

class LayerFile:
    #f_lst = []
    def __init__(self, layer_id, size):
        self.layer_id = layer_id
        self.size = size
	self.f_lst = []


def find_files():
    manager = multiprocessing.Manager()
    layer_dict = manager.dict()
    extracting_dir_size = manager.Value('i', 50*1000*1000*1000)

    layerfile_lst_less_50m = []
    layerfile_lst_less_1g = []
    layerfile_lst_less_2g = []
    layerfile_lst_bigger_2g = []

    pnum_less_50m = 60
    pnum_less_1g = 30
    pnum_less_2g = 15
    pnum_bigger_2g = 5

    if not construct_layer_info_or_not:
        layer_dict = construct_layer_info()
    else:
        with open(layer_dict_fname) as f:
            tmp_dict = json.load(f)
        for layer_id, layer_fname in tmp_dict.items():
            layer_dict[layer_id] = layer_fname

    layer_file_dict = find_layer_filename()
    #len1 = 0
    #for layer_id in layer_file_dict.keys():
    #    print layer_file_dict[layer_id].layer_id
    #    print layer_file_dict[layer_id].size
    #    print layer_file_dict[layer_id].f_lst[0]
    #    len1 = len1 + len(layer_file_dict[layer_id].f_lst)
    #print len1
    #    #break
    #return

    logging.info('Initializing the job lists....')

    for layerfile in (sorted(layer_file_dict.values(), key=operator.attrgetter('size'))):
        if layerfile.size <= 50*1024*1024:
            layerfile_lst_less_50m.append(layerfile)
        elif layerfile.size <= 1*1024*1024*1024:
            layerfile_lst_less_1g.append(layerfile)
        elif layerfile.size <= 2*1024*1024*1024:
            layerfile_lst_less_2g.append(layerfile)
        else:
            layerfile_lst_bigger_2g.append(layerfile)

    #print layerfile_lst_less_50m[0].size
    #print layerfile_lst_less_1g[0].size
    #print layerfile_lst_less_2g[0].size
    #print layerfile_lst_bigger_2g[0].size

    #return
    
    err_lst_less_50m = process_layerfile_list(layerfile_lst_less_50m, 'layerfile_lst_less_50m', layer_dict, extracting_dir_size, pnum_less_50m)
    while err_lst_less_50m:
        pnum_less_50m = pnum_less_50m - 5
        if pnum_less_50m <= 0:
            pnum_less_50m = 5
	logging.debug('processing error list < 50m....')
        err_lst_less_50m = process_layerfile_list(err_lst_less_50m, 'layerfile_lst_less_50m', layer_dict, extracting_dir_size, pnum_less_50m)
    #return
    
    err_lst_less_1g = process_layerfile_list(layerfile_lst_less_1g, 'layerfile_lst_less_1g', layer_dict, extracting_dir_size, pnum_less_1g)
    #return
    while err_lst_less_1g:
        pnum_less_1g = pnum_less_1g - 5
        if pnum_less_1g <= 0:
            pnum_less_1g = 4
	logging.debug('processing error list < 1g....')
        err_lst_less_1g = process_layerfile_list(err_lst_less_1g, 'layerfile_lst_less_1g', layer_dict, extracting_dir_size, pnum_less_1g)
    #return
    err_lst_less_2g = process_layerfile_list(layerfile_lst_less_2g, 'layerfile_lst_less_2g', layer_dict, extracting_dir_size, pnum_less_2g)
    logging.debug('processing error list < 2g....')
    while err_lst_less_2g:
        pnum_less_2g = pnum_less_2g - 5
        if pnum_less_2g <= 0:
            pnum_less_2g = 3
        err_lst_less_2g = process_layerfile_list(err_lst_less_2g, 'layerfile_lst_less_2g', layer_dict, extracting_dir_size, pnum_less_2g)

    err_lst_bigger_2g = process_layerfile_list(layerfile_lst_bigger_2g, 'layerfile_lst_bigger_2g', layer_dict, extracting_dir_size, pnum_bigger_2g)
    logging.debug('processing error list > 5g....')
    while err_lst_bigger_2g:
        pnum_bigger_2g = pnum_bigger_2g - 1
        if pnum_bigger_2g <= 0:
            pnum_bigger_2g = 2
        err_lst_bigger_2g = process_layerfile_list(err_lst_bigger_2g, 'layerfile_lst_bigger_2g', layer_dict, extracting_dir_size, pnum_bigger_2g)

    logging.debug('done! all the layer job processes are finished')


def process_layerfile_list(layerfile_lst, lst_type, layer_dict, extracting_dir_size, num):
    error_ret = []
    if not layerfile_lst:
        return error_ret

    func = partial(load_dirs, layer_dict, extracting_dir_size)

    logging.debug("layerfile_lst size: %d ....", len(layerfile_lst))

    sublists = split_list(layerfile_lst, num)

    logging.debug( "create pool to decompress and unpack layer tarfiles ....")
    P_unpack = multiprocessing.Pool(num)
    logging.debug( "create pool to mv layer files ....")
    P_mv = multiprocessing.Pool(60)

    for sublist in sublists:
	ret_lst = []
        
        logging.debug("map pool to decompress and unpack layer tarfiles ....")
        start = time.time()
        ret_lst = P_unpack.map(func, sublist)
	#for i in sublist:
	#    cnt = cnt + 1
	#    if cnt > 4:
	#	break
	#    ret_lst.append(func(i))
        elapsed = time.time() - start
        logging.debug('decompress and unpacking: throughput ==> %f /s', len(sublist) / elapsed)
	
        # logging.debug("finished decompressing and unpacking layer tarfiles!")

        lf_lst = []

        for ret in ret_lst:
            if not ret['errors'] and ret['finished_or_not']:
                lf_lst.extend(ret['lf_lst'])
                """write to finished lst"""
            elif ret['errors'] == 'NoSpaceLeft':
                error_ret.append(ret['layerfile'])
                """take care of bad list: ONLY FOR NOSPACELEFT"""
        #print lf_lst

        logging.debug("map pool to mv layer files ....")
        start = time.time()
        P_mv.map(load_files, lf_lst)
	#for i in lf_lst:
	#    load_files(i)
        elapsed = time.time() - start
        logging.debug('mv files: throughput ==> %f /s', len(lf_lst) / elapsed)
     #    logging.debug("finished mv-ing layer tarfiles!")

        clear_extracting_dir(dest_dir[0]['extracting_dir'])

        extracting_dir_size.value = 50 * 1000 * 1000 * 1000
    logging.debug("start close pool")
    P_unpack.close()
    P_mv.close()
    logging.debug("start joinning workers....")
    P_unpack.join()
    P_mv.join()

    return error_ret


def load_dirs(layer_dict, extracting_dir_size, layerfile):
    processname = multiprocessing.current_process().name
    # layer_id,  = dict_item
    layer_id = layerfile.layer_id
    file_lst = layerfile.f_lst
    # size = layerfile.size

    logging.info("[%s] process layer_filename: %s", processname, layer_id)
    #print layer_dict
    #return False
    ret = {
        'finished_or_not': False,
        'lf_lst': [],
        'layerfile': layerfile,
        # 'layer_dir': None,
        'errors': None
    }

    try:
        layer_absfilename = layer_dict[layer_id]
        #print layer_absfilename
    except:
        #pass
        logging.debug("Cannot find layer_id in this machine's layer directories ################ ", layer_id)
        ret['errors'] = 'NoneLayerFound'
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
    if not mk_dir(layer_dir):
        ret['errors'] = 'MakeDirError'
        return ret

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

    logging.info('cp tarball to ==========> %s', layer_dir)

    sum_size = 0

    layer_compressed_size = os.lstat(layer_file).st_size

    if (extracting_dir_size.value - layer_compressed_size) <= 0:
        logging.debug("NO space left: %d", extracting_dir_size.value)
        ret['errors'] = 'NoSpaceLeft'
        return ret
	# # Wait for 5 seconds
	# time.sleep(50)

    extracting_dir_size.value = extracting_dir_size.value - layer_compressed_size
    sum_size = sum_size + layer_compressed_size

    if not cp_file(layer_file, cp_layer_tarball_name):
        clear_dir(layer_dir)
        extracting_dir_size.value = extracting_dir_size.value + sum_size
        ret['errors'] = 'CopyFileError'
        return ret
    
    #logging.debug('SPACE LEFT ==========> %d: sum_size:%d', extracting_dir_size.value/1024/1024/1024, sum_size/1024/1024/1024)

    if not os.path.isfile(cp_layer_tarball_name):
        logging.warn('cp layer tarball file %s is invalid', cp_layer_tarball_name)
        clear_dir(layer_dir)
        extracting_dir_size.value = extracting_dir_size.value + sum_size
        ret['errors'] = 'InvalidFileError'
        return ret

    if filetype == 'gzip':
        logging.info('STAT Extracting gzip file ==========> %s' % layer_file)
        abs_zip_file_name = os.path.join(extracting_dir, layer_filename + '-uncompressed-archival.tar')
        if extracting_dir_size.value - layer_compressed_size * 4 <= 0: #### make a guess
            logging.debug("NO space left: %d", extracting_dir_size.value)
            ret['errors'] = 'NoSpaceLeft'
            return ret
	    # time.sleep(50)

        #size = os.lstat(abs_zip_file_name).st_size

        extracting_dir_size.value = extracting_dir_size.value - layer_compressed_size * 4
        sum_size = sum_size + layer_compressed_size * 4                                          


        if not decompress_tarball_gunzip(cp_layer_tarball_name, abs_zip_file_name): # gunzip decompression
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            ret['errors'] = 'NoSpaceLeft' # ret['errors'] = 'DecompressError'
            return ret

        size = os.lstat(abs_zip_file_name).st_size

        extracting_dir_size.value = extracting_dir_size.value - size + layer_compressed_size * 4
        sum_size = sum_size + size - layer_compressed_size * 4

        if os.path.isfile(abs_zip_file_name):
            # uncompressed_archival_size = os.lstat(abs_zip_file_name).st_size
            logging.info("uncompressed_archival_size %d B, name: %s", size, layer_file)
        else:
            logging.debug("uncompressed_archival_wrong!!! name: %s", layer_file)
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            ret['errors'] = 'InvalidFileError'
            return ret

        if extracting_dir_size.value - size <= 0: #### archival == unpacking
            logging.debug("NO space left: %d", extracting_dir_size.value)
            ret['errors'] = 'NoSpaceLeft'
            return ret
	    # time.sleep(50)

        extracting_dir_size.value = extracting_dir_size.value - size
        sum_size = sum_size + size                                          

	if not extract_tarball(abs_zip_file_name, layer_dir):
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            ret['errors'] = 'NoSpaceLeft' #ret['errors'] = 'UnpackError'
            return ret
        
        #logging.debug('SPACE LEFT ==========> %d: sum_size:%d', extracting_dir_size.value / 1024 / 1024 / 1024, sum_size / 1024 / 1024 / 1024)

        if not os.path.isdir(layer_dir):
            logging.warn('layer dir %s is invalid', layer_dir)
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            ret['errors'] = 'InvalidDirError'
            return ret

        if not remove_file(cp_layer_tarball_name):
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            ret['errors'] = 'RmError'
            return ret

        extracting_dir_size.value = extracting_dir_size.value + layer_compressed_size
        sum_size = sum_size - layer_compressed_size

        #logging.debug('SPACE LEFT ==========> %d: sum_size:%d', extracting_dir_size.value / 1024 / 1024 / 1024, sum_size / 1024 / 1024 / 1024)

    elif filetype == 'tar':
        logging.info('STAT Extracting tar file ==========> %s' % layer_file)

        if extracting_dir_size.value - layer_compressed_size <= 0: #### archival == unpacking
            print("NO space left: %d", extracting_dir_size.value)
            ret['errors'] = 'NoSpaceLeft'
	    # time.sleep(50)

        extracting_dir_size.value = extracting_dir_size.value - layer_compressed_size
        sum_size = sum_size + layer_compressed_size                                          

	if not extract_tarball(cp_layer_tarball_name, layer_dir):
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            ret['errors'] = 'NoSpaceLeft'
            # ret['errors'] = 'UnpackError'
            return ret

        #logging.debug('SPACE LEFT ==========> %d: sum_size:%d', extracting_dir_size.value / 1024 / 1024 / 1024,  sum_size / 1024 / 1024 / 1024)

        if not os.path.isdir(layer_dir):
            logging.warn('layer dir %s is invalid', layer_dir)
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            ret['errors'] = 'InvalidDirError'
            return ret

        if not remove_file(cp_layer_tarball_name):
            clear_dir(layer_dir)
            extracting_dir_size.value = extracting_dir_size.value + sum_size
            ret['errors'] = 'RmError'
            return ret

        extracting_dir_size.value = extracting_dir_size.value + layer_compressed_size
        sum_size = sum_size - layer_compressed_size

        #logging.debug('SPACE LEFT ==========> %d: sum_size:%d', extracting_dir_size.value / 1024 / 1024 / 1024, sum_size / 1024 / 1024 / 1024)

    tup_lst = []

    for fname in file_lst:
        file_name = fname.split('/',1)[1]
        dirname = os.path.dirname(fname)
        dir_name = dirname.split('/',1)[1]
        if os.path.isfile(os.path.join(layer_dir, file_name)):
            file_dirname = os.path.join(output_dir_layer_dir, dir_name)
            # if not os.path.isdir(file_dirname):
            #     mk_dir(file_dirname)

            tup = (os.path.join(layer_dir, file_name), file_dirname)
            tup_lst.append(tup)
    #print tup_lst
    ret = {
        'finished_or_not': True,
        'lf_lst': tup_lst, #file_lst,
        'layerfile': layerfile, #tup_lst, # layer_absfname, des_dirname
        # 'layer_dir': layer_dir,
        'errors': None
    }

    return ret
    # ret = load_files(layer_dir, file_lst, output_dir_layer_dir)
    #if not ret:
    # clear_dir(layer_dir)

    #logging.debug('SPACE LEFT ==========> %d: sum_size:%d', extracting_dir_size.value/1024/1024/1024, sum_size/1024/1024/1024)
    # return ret


def load_files(layerfile_tup):
    start = time.time()
    #print layerfile_tup
    src_absfname = layerfile_tup[0]
    dest_dir = layerfile_tup[1]
    if not os.path.isdir(dest_dir):
	mk_dir(dest_dir)
    #print src_absfname, dest_dir
    mv_files(src_absfname, dest_dir)

    # for fname in file_lst:
    #     file_name = fname.split('/',1)[1]
    #     dirname = os.path.dirname(fname)
    #     dir_name = dirname.split('/',1)[1]
    #     if os.path.isfile(os.path.join(layer_dir, file_name)):
    #         file_dirname = os.path.join(output_dir, dir_name)
    #         if not os.path.isdir(file_dirname):
    #             mk_dir(file_dirname)

            # mv_files(os.path.join(layer_dir, file_name), file_dirname)

    elapsed = time.time() - start
    logging.info('process layer_id:%s : file move for layer dest dir, ==> %f s', dest_dir, elapsed)

    return True


def construct_layer_info():
    layer_dict = {}
    logging.debug('construct layer info: a mapping between layer_id and layer_absfname....')

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

    logging.debug('construct layer filename dict: a dict contains layer_id, layer_size, and layer_flst....')

    layer_file_ldict = {}
    with open(find_file_lst_absfilename) as f:
        for line in f:
            line = line.rstrip('\n')
            layer_id = line.split(',', 2)[0] # split by \t ???????
            size     = int(line.split(',', 2)[1])
            filename = line.split(',', 2)[2] # split by \t ???????
            if layer_id not in layer_file_ldict:
                layer_file_ldict[layer_id] = LayerFile(layer_id, size)
            layer_file = layer_file_ldict[layer_id]
	    layer_file.f_lst.append(filename)

    return layer_file_ldict
