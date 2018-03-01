#import cStringIO
from config import *
#from draw_pic import *
#from utility import *
from file import *

"""
TODO:
1. get compression
2. check
"""

def archival_tarfile(abs_zip_file_name, layer_dir):
    """ first archive this dir """
    start = time.time()

    cmd = 'tar -cf %s %s' % (abs_zip_file_name, layer_dir)
    logging.debug('The shell command: %s', cmd)
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.error('###################%s: exit code: %s; %s###################',
                      abs_zip_file_name, e.returncode, e.output)
        return False

    elapsed = time.time() - start
    logging.info('process layer_id:%s : tar archival directory, consumed time ==> %f s', abs_zip_file_name, elapsed)
    return True


def compress_tarball_gzip(abs_tar_file_name, abs_gzip_filename): #.gz
    start = time.time()

    cmd = 'gzip -f %s' % (abs_tar_file_name)
    logging.debug('The shell command: %s', cmd)
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.error('###################%s: exit code: %s; %s###################',
                      abs_tar_file_name, e.returncode, e.output)
        return False

    elapsed = time.time() - start
    logging.info('process layer_id:%s : gzip compress tar archival, consumed time ==> %f s', abs_tar_file_name, elapsed)
    return True


def clear_dir(layer_dir):
    """ delete the dir """
    if not os.path.isdir(layer_dir):
        logging.error('###################%s is not valid###################', layer_dir)
        return False

    cmd4 = 'rm -rf %s' % (layer_dir+'*')
    logging.debug('The shell command: %s', cmd4)
    try:
        subprocess.check_output(cmd4, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.error('###################%s: exit code: %s; %s###################',
                      dir, e.returncode, e.output)
        return False

    return True


def remove_file(absfilename):
    if not os.path.isfile(absfilename):
        logging.error('###################%s is not valid###################', absfilename)
        return False

    cmd4 = 'rm -rf %s' % absfilename
    logging.debug('The shell command: %s', cmd4)
    try:
        subprocess.check_output(cmd4, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.error('###################%s: exit code: %s; %s###################',
                      dir, e.returncode, e.output)
        return False

    return True


def extract_tarball(layer_dir_filename, layer_dir):
    start = time.time()
    cmd = 'tar -pxf %s -C %s' % (layer_dir_filename, layer_dir)
    logging.debug('The shell command: %s', cmd)
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        if "Unexpected EOF in archive" in e.output:
            logging.debug('###################%s: Pass exit code: %s; %s###################',
                      layer_dir_filename, e.returncode, e.output)
        else:
            logging.debug('###################%s: exit code: %s; %s###################',
                          layer_dir_filename, e.returncode, e.output)
            #return False

    elapsed = time.time() - start
    logging.info('process layer_id:%s : tar extract tarball, consumed time ==> %f s', layer_dir_filename, elapsed)
    logging.debug('FINISHED! to ==========> %s', layer_dir)
    return True


def decompress_tarball_gunzip(cp_layer_tarball_name, layer_dir_filename):
    start = time.time()
    cmd = 'gunzip -c %s > %s' % (cp_layer_tarball_name, layer_dir_filename)
    logging.debug('The shell command: %s', cmd)
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.debug('###################%s: exit code: %s; %s###################',
                      cp_layer_tarball_name, e.returncode, e.output)
        return False

    elapsed = time.time() - start
    logging.info('process layer_id:%s : gunzip decompress tarball, consumed time ==> %f s', cp_layer_tarball_name, elapsed)
    logging.debug('FINISHED! to ==========> %s', layer_dir_filename)
    return True


def mk_dir(layer_dir):
    cmd1 = 'mkdir %s' % layer_dir
    logging.debug('The shell command: %s', cmd1)
    try:
        subprocess.check_output(cmd1, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.debug('###################%s: %s###################',
                      layer_dir, e.output)
        return False
    return True


def cp_file(layer_file, cp_layer_tarball_name):
    cmd = 'cp %s  %s' % (layer_file, cp_layer_tarball_name)
    logging.debug('The shell command: %s', cmd)
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.debug('###################%s: %s###################',
                      layer_file, e.output)
        return False
    return True


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

    if not load_files(layer_dir, sub_dirs, layer_dir_level):
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


def sum_dir_size(s_dir_files):
    sum_size = 0
    for file in s_dir_files:
        sum_size = file['file_info']['stat_size'] + sum_size
    return sum_size


def load_files(layer_dir, sub_dirs, layer_dir_level):
    start = time.time()

    all_dirs = []
    all_files = []

    try:
        for path, subdirs, files in os.walk(layer_dir):
            if len(subdirs):
                for dirname in subdirs:
                    s_dir = os.path.join(path, dirname)
                    if os.path.isdir(s_dir):
                        all_dirs.append(s_dir.replace(layer_dir, ""))

                for f in files:
                    try:
                        filename = os.path.isfile(os.path.join(path, f))
                    except:
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                                  limit=2, file=sys.stdout)
                        continue

                    if os.path.isfile(os.path.join(path, f)):
                        s_dir_file = load_file(os.path.join(path, f), path)
			#s_dir_file.filename = os.path.join(path, s_dir_file.filename).replace(layer_dir, "")
                        if s_dir_file:
                            all_files.append(s_dir_file)

        sub_dirs = {
            'subdirs': all_dirs,
            'file_cnt': len(all_files),
            'files': all_files,  # full path of f = dir/files
            'dir_size': sum_dir_size(all_files)
        }
                #sub_dirs.append(sub_dir)

    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  limit=2, file=sys.stdout)
        return False

    elapsed = time.time() - start
    logging.info('process layer_id:%s : file digest calculation for layer, ==> %f s', layer_dir, elapsed)

    return True
