
from config import *
from dir import *


def decompress_tarball_lz4(cp_layer_tarball_name, layer_dir_filename):
    start = time.time()
    cmd = 'unlz4 %s > %s' % (cp_layer_tarball_name, layer_dir_filename)
    logging.debug('The shell command: %s', cmd)
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.debug('###################%s: exit code: %s; %s###################',
                      cp_layer_tarball_name, e.returncode, e.output)
        return False

    elapsed = time.time() - start
    logging.info('process layer_id:%s : unlz4 decompress tarball, consumed time ==> %f s', cp_layer_tarball_name, elapsed)
    logging.debug('FINISHED! to ==========> %s', layer_dir_filename)
    return True


def compress_tarball_lz4(abs_tar_file_name, abs_gzip_filename): #.gz #.lz4
    start = time.time()

    cmd = 'lz4 %s' % (abs_tar_file_name)
    logging.debug('The shell command: %s', cmd)
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.error('###################%s: exit code: %s; %s###################',
                      abs_tar_file_name, e.returncode, e.output)
        return False

    elapsed = time.time() - start
    logging.info('process layer_id:%s : lz4 compress tar archival, consumed time ==> %f s', abs_tar_file_name, elapsed)
    return True


def compress_tarball_with_method(tarball_fname, outputname, method):
    if method == 'gzip':
        return compress_tarball_gzip(tarball_fname, outputname)
    elif method == 'lz4':
        return compress_tarball_lz4(tarball_fname, outputname)
    else:
        logging.error('###################%s: unknown compression method ###################',
                      method)


def decompress_tarball_with_method(tarball_fname, outputname, method):
    if method == 'gzip':
        decompress_tarball_gunzip(tarball_fname, outputname)
    elif method == 'lz4':
        decompress_tarball_lz4(tarball_fname, outputname)
    else:
        logging.error('###################%s: unknown compression method ###################',
                      method)


def load_dirs(layer_absfilename, filetype):
    # sub_dirs = {}

    """ load the layer file in layer file dest_dir['layer_dir']/<layer_id>
    extracting to extracting_dir/<layer_id>
    load all the subdirs in this layer-id dir
    Then clean the layer-id dir """
    uncompressed_archival_size = -1
    compressed_archival_size = -1

    layer_filename = os.path.basename(layer_absfilename)
    layer_file = layer_absfilename  #os.path.join(dest_dir[0]['layer_dir'], layer_filename)

    extracting_dir = dest_dir[0]['extracting_dir']
    layer_dir = os.path.join(extracting_dir, layer_filename)
    if start_status_compressed == True:
    	mk_dir(layer_dir)

    cp_layer_tarball_name = os.path.join(extracting_dir, layer_filename + '-cp.tar.gzip')
    abs_archival_file_name = os.path.join(extracting_dir, layer_filename + '-archival.tar')

    """
    extracting_dir:
                    layer_dir: layer_filename
                    cp_layer_tarball_name: layer_filename-cp.tar.zip
                    abs_zip_file_name: layer_filename-uncompressed-archival.tar
                    gzip: .tar.zip
                    lz4: .tar.lz4
    """

    logging.debug('cp tarball to ==========> %s', layer_dir)
    if start_status_compressed == True or start_status_compressed == False:
        if not cp_file(layer_file, cp_layer_tarball_name):
            clear_dir(layer_dir)
            return -1, -1

        if not os.path.isfile(cp_layer_tarball_name):
            logging.warn('cp layer tarball file %s is invalid', cp_layer_tarball_name)
            clear_dir(layer_dir)
            return -1, -1

    if filetype == 'gzip':
        logging.debug('STAT Extracting gzip file ==========> %s' % layer_file)
        abs_zip_file_name = os.path.join(extracting_dir, layer_filename + '-uncompressed-archival.tar')

        if start_status_compressed == True:
            if not decompress_tarball_gunzip(cp_layer_tarball_name, abs_zip_file_name): # gunzip decompression
                clear_dir(layer_dir)
                return -1, -1

            if os.path.isfile(abs_zip_file_name):
                uncompressed_archival_size = os.lstat(abs_zip_file_name).st_size
                logging.debug("uncompressed_archival_size %d B, name: %s", uncompressed_archival_size, layer_file)
            else:
                logging.debug("uncompressed_archival_wrong!!! name: %s", layer_file)
                clear_dir(layer_dir)
                return -1, -1

            if not extract_tarball(abs_zip_file_name, layer_dir):
                clear_dir(layer_dir)
                return -1, -1

            if not os.path.isdir(layer_dir):
                logging.warn('layer dir %s is invalid', layer_dir)
                clear_dir(layer_dir)
                return -1, -1

            if not remove_file(cp_layer_tarball_name):
                clear_dir(layer_dir)
                return -1, -1

            if filetype == 'tar':
                #clear_dir(layer_dir)
                return compressed_archival_size, -1
            elif filetype == 'gzip':
                #clear_dir(layer_dir)
                return compressed_archival_size, uncompressed_archival_size

    elif filetype == 'tar':
        logging.debug('STAT Extracting tar file ==========> %s' % layer_file)
        if start_status_compressed == True:
            if not extract_tarball(cp_layer_tarball_name, layer_dir):
                clear_dir(layer_dir)
                return -1, -1

            if not os.path.isdir(layer_dir):
                logging.warn('layer dir %s is invalid', layer_dir)
                clear_dir(layer_dir)
                return -1, -1

            if filetype == 'tar':
                #clear_dir(layer_dir)
                return compressed_archival_size, -1
            elif filetype == 'gzip':
                #clear_dir(layer_dir)
                return compressed_archival_size, uncompressed_archival_size

    if start_status_compressed == False:

        """archival layer dir"""
        if not archival_tarfile(abs_archival_file_name, layer_dir):
            clear_dir(layer_dir)
            return -1, -1

        if method == 'lz4':
            """compress tar with lz4 and remove it"""
            compressed_fname = abs_archival_file_name + '.lz4'
            if not compress_tarball_with_method(abs_archival_file_name, compressed_fname, 'lz4'):
                clear_dir(layer_dir)
                return -1, -1

        if method == 'gzip':
            """compress tar and remove it"""
            compressed_fname = abs_archival_file_name + '.gz'
            if not compress_tarball_with_method(abs_archival_file_name, compressed_fname, 'gzip'):
                clear_dir(layer_dir)
                return -1, -1

        if os.path.isfile(compressed_fname):
            compressed_archival_size = os.lstat(compressed_fname).st_size
            logging.debug("%s: compressed_archival_size %d B, name: %s", method, compressed_archival_size, layer_file)
        else:
            logging.debug("%s: compressed_archival_wrong!!! name: %s", method, layer_file)
            clear_dir(layer_dir)
            return -1, -1

        if not remove_file(method):
            clear_dir(layer_dir)
            return -1, -1

        if filetype == 'tar':
            clear_dir(layer_dir)
            return compressed_archival_size, -1
        elif filetype == 'gzip':
            clear_dir(layer_dir)
            return compressed_archival_size, uncompressed_archival_size
