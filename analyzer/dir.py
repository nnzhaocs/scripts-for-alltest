
from imports import *
from draw_pic import *
from utility import *
from file import *


def zipit(path, archname):
    archive = zipfile.ZipFile(archname, "w", zipfile.ZIP_STORED)
    if os.path.isdir(path):
        _zippy(path, path, archive)
    else:
        _, name = os.path.split(path)
        archive.write(path, name)
    archive.close()


def _zippy(base_path, path, archive):
    paths = os.listdir(path)
    for p in paths:
        p = os.path.join(path, p)
        if os.path.isdir(p):
            _zippy(base_path, p, archive)
        else:
            archive.write(p, os.path.relpath(p, base_path))


def clear_dirs(layer_id, extracting_dir):
    """ delete the dir """
    start = time.time()
    layer_dir = os.path.join(extracting_dir, layer_id)
    if not os.path.isdir(layer_dir):
        logging.error('####################layer tarball dir %s is not valid############', layer_dir)
        return

    """ first archive this dir """
    start = time.time()
    abs_zip_file_name = os.path.join(layer_dir, layer_id+'-uncompressed-archival.zip')
    zipit(layer_dir, abs_zip_file_name)
    elapsed = time.time() - start
    logging.info('archival directory, consumed time ==> %f s', elapsed)

    if os.path.isfile(abs_zip_file_name):
        uncompressed_archival_size = os.lstat(abs_zip_file_name).st_size
        logging.debug("uncompressed_archival_size %d B, name: %s", uncompressed_archival_size, layer_id)
    else:
        uncompressed_archival_size = -1
        logging.debug("uncompressed_archival_wrong!!! name: %s", layer_id)

    cmd4 = 'rm -rf %s' % layer_dir
    logging.debug('The shell command: %s', cmd4)
    try:
        subprocess.check_output(cmd4, shell=True)
    except subprocess.CalledProcessError as e:
        print '###################'+e.output+'#######################'
    elapsed = time.time() - start
    logging.info('clear dirs, consumed time ==> %f s', elapsed)

    return uncompressed_archival_size


def load_dirs(layer_id, extracting_dir):
    """ load the layer file in layer file dest_dir['layer_dir']/<layer_id>
    extracting to temp/<layer_id>
    load all the subdirs in this layer-id dir
    Then clean the layer-id dir """
    sub_dirs = []

    layer_file = os.path.join(dest_dir[0]['layer_dir'], layer_id)
    logging.debug('Extracting the file ==========> %s' % layer_file)

    layer_dir = os.path.join(extracting_dir, layer_id)
    cmd1 = 'mkdir %s' % layer_dir
    logging.debug('The shell command: %s', cmd1)
    try:
        subprocess.check_output(cmd1, shell=True)
    except subprocess.CalledProcessError as e:
        print '###################'+e.output+'###################'
        q_bad_unopen_layers.put('sha256:' + layer_id.split("-")[1]+':cannot-make-dir-error')
        return sub_dirs
    logging.debug('to ==========> %s', layer_dir)

    start = time.time()
    cmd = 'tar -zxf %s -C %s' % (layer_file, layer_dir)
    logging.debug('The shell command: %s', cmd)
    try:
        subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        print '###################' + e.output + '###################'
        if "No space left on device" in e.output:
            q_bad_unopen_layers.put('sha256:' + layer_id.split("-")[1] + ':tar-no-space-error')
            return sub_dirs
        else:
            q_bad_unopen_layers.put('sha256:' + layer_id.split("-")[1] + ':tar-common-error')
    elapsed = time.time() - start
    logging.info('decompress tarball, consumed time ==> %f s', elapsed)

    layer_dir_level = layer_dir.count(os.sep)
    logging.debug("(%s, %s)", layer_dir, layer_dir_level)

    if not os.path.isdir(layer_dir):
        logging.warn('layer dir %s is invalid', layer_dir)
        return sub_dirs

    for path, subdirs, files in os.walk(layer_dir):
        for dirname in subdirs:
            s_dir = os.path.join(path, dirname)
            if not os.path.isdir(s_dir):
                logging.warn('###################layer subdir %s is invalid###################', s_dir.replace(layer_dir, ""))
                q_bad_unopen_layers.put('sha256:' + layer_id.split("-")[1]+':layer-subdir-error:'+s_dir.replace(layer_dir, ""))
                continue

            dir_level = s_dir.count(os.sep) - layer_dir_level
            s_dir_files = []
            for f in os.listdir(s_dir):
                if os.path.isfile(os.path.join(s_dir, f)):
                    s_dir_file = load_file(os.path.join(s_dir, f))
                    s_dir_files.append(s_dir_file)

            sub_dir = {
                'subdir': s_dir,  # .replace(layer_dir, ""),
                'dir_depth': dir_level,
                'file_cnt': len(s_dir_files),
                'files': s_dir_files,  # full path of f = dir/files
                'dir_size': sum_dir_size(s_dir_files)
            }

            sub_dirs.append(sub_dir)
    return sub_dirs


def sum_dir_size(s_dir_files):
    sum_size = 0
    for file in s_dir_files:
        sum_size = file['size (B)'] + sum_size
    return sum_size
