import cStringIO
from imports import *
from draw_pic import *
from utility import *
from file import *

"""
TODO:
1. get compression
2. check

"""


def clear_dir(layer_id, extracting_dir):
    """ delete the dir """
    layer_dir = os.path.join(extracting_dir, layer_id)
    if not os.path.isdir(layer_dir):
        logging.error('####################layer tarball dir %s is not valid############', layer_dir)
        return -1

    """ first archive this dir """
    start = time.time()
    abs_zip_file_name = os.path.join(layer_dir, layer_id+'-uncompressed-archival.zip')

    tar = tarfile.open(abs_zip_file_name, "w")
    tar.add(layer_dir, recursive=True)
    tar.close()
    elapsed = time.time() - start
    logging.info('archival directory, consumed time ==> %f s', elapsed)

    if os.path.isfile(abs_zip_file_name):
        uncompressed_archival_size = os.lstat(abs_zip_file_name).st_size
        logging.debug("uncompressed_archival_size %d B, name: %s", uncompressed_archival_size, layer_id)
    else:
        uncompressed_archival_size = -1
        logging.debug("uncompressed_archival_wrong!!! name: %s", layer_id)

    start = time.time()
    cmd4 = 'rm -rf %s' % layer_dir
    logging.debug('The shell command: %s', cmd4)
    try:
        subprocess.check_output(cmd4, shell=True)
    except subprocess.CalledProcessError as e:
        print '###################'+e.output+'#######################'
    elapsed = time.time() - start
    logging.info('clear dirs, consumed time ==> %f s', elapsed)

    return uncompressed_archival_size


def get_tarfile_type(type):
    if type == tarfile.REGTYPE:  # tarfile.DIRTYPE
        return "REGTYPE"
    if type == tarfile.AREGTYPE:  # tarfile.DIRTYPE
        return "AREGTYPE"
    if type == tarfile.LNKTYPE:  # tarfile.DIRTYPE
        return "LNKTYPE"
    if type == tarfile.SYMTYPE:  # tarfile.DIRTYPE
        return "SYMTYPE"
    if type == tarfile.DIRTYPE:  # tarfile.DIRTYPE
        return "DIRTYPE"
    if type == tarfile.FIFOTYPE:  # tarfile.DIRTYPE
        return "FIFOTYPE"
    if type == tarfile.CONTTYPE:  # tarfile.DIRTYPE
        return "CONTTYPE"
    if type == tarfile.CHRTYPE:  # tarfile.DIRTYPE
        return "CHRTYPE"
    if type == tarfile.BLKTYPE:  # tarfile.DIRTYPE
        return "BLKTYPE"
    if type == tarfile.GNUTYPE_SPARSE:  # tarfile.DIRTYPE
        return "GNUTYPE_SPARSE"


def load_dirs(layer_filename):
    sub_dirs = []
    dir_files = defaultdict(list)
    file_infos = {}
    files = {}

    """ load the layer file in layer file dest_dir['layer_dir']/<layer_id>
    extracting to extracting_dir/<layer_id>
    load all the subdirs in this layer-id dir
    Then clean the layer-id dir """

    layer_file = os.path.join(dest_dir[0]['layer_dir'], layer_filename)
    logging.debug('Extracting the file ==========> %s' % layer_file)

    extracting_dir = dest_dir[0]['extracting_dir']
    layer_dir = os.path.join(extracting_dir, layer_filename)
    cmd1 = 'mkdir %s' % layer_dir
    logging.debug('The shell command: %s', cmd1)
    try:
        subprocess.check_output(cmd1, shell=True)
    except subprocess.CalledProcessError as e:
        print '###################' + e.output + '###################'
        logging.debug('sha256:' + layer_filename.split("-")[1] + ':cannot-make-dir-error')
        uncompressed_archival_size = clear_dir(layer_filename, extracting_dir)
        return sub_dirs, uncompressed_archival_size

    logging.debug('to ==========> %s', layer_dir)

    start = time.time()
    cmd = 'tar -zxf %s -C %s' % (layer_file, layer_dir)
    logging.debug('The shell command: %s', cmd)
    try:
        subprocess.check_output(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        print '###################' + e.output + '###################'
        if "No space left on device" in e.output:
            logging.debug('sha256:' + layer_filename.split("-")[1] + ':tar-no-space-error')
            uncompressed_archival_size = clear_dir(layer_filename, extracting_dir)
            return sub_dirs, uncompressed_archival_size
        else:
            logging.debug('sha256:' + layer_filename.split("-")[1] + ':tar-common-error')

    elapsed = time.time() - start
    logging.info('decompress tarball, consumed time ==> %f s', elapsed)

    if not os.path.isdir(layer_dir):
        logging.warn('layer dir %s is invalid', layer_dir)
        uncompressed_archival_size = clear_dir(layer_filename, extracting_dir)
        return sub_dirs, uncompressed_archival_size

    try:
        tar = tarfile.open(layer_file, "r")
    except tarfile.TarError:
        logging.error("###################Cannot open tarfile: %s is wrong.###################", layer_filename)
        print tarfile.TarError
        uncompressed_archival_size = clear_dir(layer_filename, extracting_dir)
        return sub_dirs, uncompressed_archival_size

    for tarinfo in tar:
        sha256 = None
        f_type = None
        extension = None
        link = None
        # logging.debug(tarinfo.name + ' , %d', tarinfo.size)
        if tarinfo.issym():
            link = {
                'link_type': 'symlink',
                'target_path': tarinfo.linkname
            }
        if tarinfo.islnk():
            link = {
                'link_type': 'hardlink',
                'target_path': tarinfo.linkname
            }
        if tarinfo.isdir():
            # logging.debug("dirname : %s", tarinfo.name)
            dir_level = tarinfo.name.count(os.sep) + 1
            sub_dir = {
                'dirname': tarinfo.name,  # .replace(layer_dir, ""),
                'dir_depth': dir_level
                # 'file_cnt': len(s_dir_files),
                # 'files': s_dir_files,  # full path of f = dir/files
                # 'dir_size': sum_dir_size(s_dir_files)
            }
            sub_dirs.append(sub_dir)

        if tarinfo.isreg():
	    try:
		f_name = os.path.join(layer_dir, tarinfo.name)
	    except UnicodeDecodeError as e:
		logging.error("############## wrong file name %s ##############", e)
	        tarinfo.name = tarinfo.name.decode('utf-8') 
	    if os.path.isfile(os.path.join(layer_dir, tarinfo.name)):
            	f_info = load_file(os.path.join(layer_dir, tarinfo.name))
            	sha256 = f_info['sha256']
            	f_type = f_info['type']
            	extension = f_info['extension']

        dir_file = {
            'filename': tarinfo.name,
            'sha256': sha256,
            'type': f_type,
            'extension': extension
            # 'symlink': symlink,
            # 'statinfo': statinfo
        }

        if tarinfo.isreg():
            dir_files[os.path.dirname(tarinfo.name)].append(dir_file)
            files[tarinfo.name] = dir_file

        tar_info = {
            # 'st_nlink': stat.st_nlink,
            'ti_size': tarinfo.size,
            'ti_type': get_tarfile_type(tarinfo.type),
            'ti_uname': tarinfo.uname,
            'ti_gname': tarinfo.gname,
            # 'ti_atime': None,  # most recent access time
            'ti_mtime': tarinfo.mtime,  # change of content
            # 'ti_ctime': None  # matedata modify
            'link': link
            # 'hardlink': hardlink
        }
        if tarinfo.isreg():
            file_infos[tarinfo.name] = tar_info

    for filename, file in files.items():
        file['file_info'] = file_infos[filename]

    for dir in sub_dirs:
        # print dir['dirname']
        dir['files'] = dir_files[dir['dirname']]
        dir['file_cnt'] = len(dir['files'])
        dir['dir_size'] = sum_dir_size(dir['files'])

    tar.close()
    uncompressed_archival_size = clear_dir(layer_filename, extracting_dir)
    return sub_dirs, uncompressed_archival_size


def sum_dir_size(s_dir_files):
    sum_size = 0
    for file in s_dir_files:
        sum_size = file['file_info']['ti_size'] + sum_size
    return sum_size
