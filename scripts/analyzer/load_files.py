
from config import *
from dir import *
from layers import check_file_type


def load_dirs(layer_filename, file_lst, output_dir):
    filetype = check_file_type(layer_filename)

    ret = False

    """ load the layer file in layer file dest_dir['layer_dir']/<layer_id>
    extracting to extracting_dir/<layer_id>
    load all the subdirs in this layer-id dir
    Then clean the layer-id dir """

    layer_file = os.path.join(dest_dir[0]['layer_dir'], layer_filename)

    extracting_dir = dest_dir[0]['extracting_dir']
    layer_dir = os.path.join(extracting_dir, layer_filename)
    mk_dir(layer_dir)
    cp_layer_tarball_name = os.path.join(extracting_dir, layer_filename + '-cp.tar.gzip')

    """
    extracting_dir:
                    layer_dir: layer_filename
                    cp_layer_tarball_name: layer_filename-cp.tar.zip
                    abs_zip_file_name: layer_filename-uncompressed-archival.tar
    """

    logging.debug('cp tarball to ==========> %s', layer_dir)
    if not cp_file(layer_file, cp_layer_tarball_name):
        clear_dir(layer_dir)
        return ret

    if not os.path.isfile(cp_layer_tarball_name):
        logging.warn('cp layer tarball file %s is invalid', cp_layer_tarball_name)
        clear_dir(layer_dir)
        return ret

    if filetype == 'gzip':
        logging.debug('STAT Extracting gzip file ==========> %s' % layer_file)
        abs_zip_file_name = os.path.join(extracting_dir, layer_filename + '-uncompressed-archival.tar')
        if not decompress_tarball_gunzip(cp_layer_tarball_name, abs_zip_file_name): # gunzip decompression
            clear_dir(layer_dir)
            return ret

        if os.path.isfile(abs_zip_file_name):
            uncompressed_archival_size = os.lstat(abs_zip_file_name).st_size
            logging.debug("uncompressed_archival_size %d B, name: %s", uncompressed_archival_size, layer_file)
        else:
            logging.debug("uncompressed_archival_wrong!!! name: %s", layer_file)
            clear_dir(layer_dir)
            return ret

        if not extract_tarball(abs_zip_file_name, layer_dir):
            clear_dir(layer_dir)
            return ret

        if not os.path.isdir(layer_dir):
            logging.warn('layer dir %s is invalid', layer_dir)
            clear_dir(layer_dir)
            return ret

        if not remove_file(cp_layer_tarball_name):
            clear_dir(layer_dir)
            return ret

    elif filetype == 'tar':
        logging.debug('STAT Extracting tar file ==========> %s' % layer_file)

        if not extract_tarball(cp_layer_tarball_name, layer_dir):
            clear_dir(layer_dir)
            return ret

        if not os.path.isdir(layer_dir):
            logging.warn('layer dir %s is invalid', layer_dir)
            clear_dir(layer_dir)
            return ret

        if not remove_file(cp_layer_tarball_name):
            clear_dir(layer_dir)
            return ret

    ret = load_files(layer_dir, file_lst, output_dir)
    if not ret:
        clear_dir(layer_dir)
        return ret


def mv_files(src_absfname, src_dir):
    cmd = 'mv %s  %s' % (src_absfname, src_dir)
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
        if os.path.isfile(os.path.join(layer_dir, file_name)):
            mv_files(os.path.join(layer_dir, file_name), output_dir)


    elapsed = time.time() - start
    logging.info('process layer_id:%s : file digest calculation for layer, ==> %f s', layer_dir, elapsed)

    return True