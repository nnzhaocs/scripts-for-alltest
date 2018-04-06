
from config import *
from dir import *



def load_dirs(layer_filename, filetype):
    sub_dirs = {}

    """ load the layer file in layer file dest_dir['layer_dir']/<layer_id>
    extracting to extracting_dir/<layer_id>
    load all the subdirs in this layer-id dir
    Then clean the layer-id dir """
    uncompressed_archival_size = -1
    compressed_archival_size = -1

    layer_file = os.path.join(dest_dir[0]['layer_dir'], layer_filename)

    extracting_dir = dest_dir[0]['extracting_dir']
    layer_dir = os.path.join(extracting_dir, layer_filename)
    mk_dir(layer_dir)
    cp_layer_tarball_name = os.path.join(extracting_dir, layer_filename + '-cp.tar.gzip')
    abs_archival_file_name = os.path.join(extracting_dir, layer_filename + '-archival.tar')
    """
    extracting_dir:
                    layer_dir: layer_filename
                    cp_layer_tarball_name: layer_filename-cp.tar.zip
                    abs_zip_file_name: layer_filename-uncompressed-archival.tar
    """

    logging.debug('cp tarball to ==========> %s', layer_dir)
    if not cp_file(layer_file, cp_layer_tarball_name):
        clear_dir(layer_dir)
        return sub_dirs, -1

    if not os.path.isfile(cp_layer_tarball_name):
        logging.warn('cp layer tarball file %s is invalid', cp_layer_tarball_name)
        clear_dir(layer_dir)
        return sub_dirs, -1

    if filetype == 'gzip':
        logging.debug('STAT Extracting gzip file ==========> %s' % layer_file)
        abs_zip_file_name = os.path.join(extracting_dir, layer_filename + '-uncompressed-archival.tar')
        if not decompress_tarball_gunzip(cp_layer_tarball_name, abs_zip_file_name): # gunzip decompression
            clear_dir(layer_dir)
            return sub_dirs, -1

        if os.path.isfile(abs_zip_file_name):
            uncompressed_archival_size = os.lstat(abs_zip_file_name).st_size
            logging.debug("uncompressed_archival_size %d B, name: %s", uncompressed_archival_size, layer_file)
        else:
            logging.debug("uncompressed_archival_wrong!!! name: %s", layer_file)
            clear_dir(layer_dir)
            return sub_dirs, -1

        if not extract_tarball(abs_zip_file_name, layer_dir):
            clear_dir(layer_dir)
            return sub_dirs, -1

        if not os.path.isdir(layer_dir):
            logging.warn('layer dir %s is invalid', layer_dir)
            clear_dir(layer_dir)
            return sub_dirs, -1

        if not remove_file(cp_layer_tarball_name):
            clear_dir(layer_dir)
            return sub_dirs, -1

        """compress tar and remove it"""
        abs_gzip_file_name = abs_zip_file_name + '.gz'
        if not compress_tarball_gzip(abs_zip_file_name, abs_gzip_file_name):
            clear_dir(layer_dir)
            return sub_dirs, -1

        if not remove_file(abs_gzip_file_name):
            clear_dir(layer_dir)
            return sub_dirs, -1

        # if not remove_file(abs_zip_file_name):
        #     clear_dir(layer_dir)
        #     return sub_dirs, -1

    elif filetype == 'tar':
        logging.debug('STAT Extracting tar file ==========> %s' % layer_file)

        if not extract_tarball(cp_layer_tarball_name, layer_dir):
            clear_dir(layer_dir)
            return sub_dirs, -1

        if not os.path.isdir(layer_dir):
            logging.warn('layer dir %s is invalid', layer_dir)
            clear_dir(layer_dir)
            return sub_dirs, -1

        """compress tar and remove it"""
        abs_gzip_file_name = cp_layer_tarball_name + '.gz'
        if not compress_tarball_gzip(cp_layer_tarball_name, abs_gzip_file_name):
            clear_dir(layer_dir)
            return sub_dirs, -1

        if os.path.isfile(abs_gzip_file_name):
            compressed_archival_size = os.lstat(abs_gzip_file_name).st_size
            logging.debug("compressed_archival_size %d B, name: %s", compressed_archival_size, layer_file)
        else:
            logging.debug("compressed_archival_wrong!!! name: %s", layer_file)
            clear_dir(layer_dir)
            return sub_dirs, -1

        if not remove_file(abs_gzip_file_name):
            clear_dir(layer_dir)
            return sub_dirs, -1

        # if not remove_file(cp_layer_tarball_name):
        #     clear_dir(layer_dir)
        #     return sub_dirs, -1

    layer_dir_level = layer_dir.count(os.sep)
    logging.debug("(%s, %s)", layer_dir, layer_dir_level)

    sub_dirs = load_files(layer_dir, layer_dir_level)
    if not sub_dirs:
        clear_dir(layer_dir)
        return sub_dirs, -1

    """archival layer dir"""
    if not archival_tarfile(abs_archival_file_name, layer_dir):
        clear_dir(layer_dir)
        return sub_dirs, -1

    if filetype == 'tar':
        clear_dir(layer_dir)
        return sub_dirs, compressed_archival_size
    elif filetype == 'gzip':
        clear_dir(layer_dir)
        return sub_dirs, uncompressed_archival_size