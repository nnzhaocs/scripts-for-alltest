
from imports import *
from draw_pic import *
from utility import *


def get_file_type(mode):
    if S_ISREG(mode):  # tarfile.DIRTYPE
        return "REGTYPE"
    # if type == tarfile.AREGTYPE:  # tarfile.DIRTYPE
    #     return "AREGTYPE"
    # if S_ISLNK(mode):  # tarfile.DIRTYPE
    #     return "LNKTYPE"
    if S_ISLNK(mode):  # tarfile.DIRTYPE
        return "SYMTYPE"
    # if type == tarfile.DIRTYPE:  # tarfile.DIRTYPE
    #     return "DIRTYPE"
    if S_ISFIFO(mode):  # tarfile.DIRTYPE
        return "FIFOTYPE"
    # if type == tarfile.CONTTYPE:  # tarfile.DIRTYPE
    #     return "CONTTYPE"
    if S_ISCHR(mode):  # tarfile.DIRTYPE
        return "CHRTYPE"
    if S_ISBLK(mode):  # tarfile.DIRTYPE
        return "BLKTYPE"
    # if type == tarfile.GNUTYPE_SPARSE:  # tarfile.DIRTYPE
    #     return "GNUTYPE_SPARSE"
    if S_ISSOCK(mode):
        return "SOCK"


def load_file(abs_filename):
    sha256 = None
    f_type = None
    extension = None
    link = None
    f_size = 0

    mode = os.stat(abs_filename).st_mode
    stat = os.stat(abs_filename)

    if S_ISLNK(mode):
        path = os.readlink(abs_filename)
        link = {
            'link_type': 'symlink',
            'target_path': path
        }

    if stat.st_nlink:
        link = {
            'link_type': 'hardlink',
            'num_hdlinks': stat.st_nlink
        }

    if get_file_type(mode) == "REGTYPE":
        sha256 = hashlib.md5(open(abs_filename, 'rb').read()).hexdigest()
        f_type = me.from_file(abs_filename)
        extension = os.path.splitext(abs_filename)[1]

        f_size = os.lstat(abs_filename).st_size
        # if f_size > 100000000000:
        #     logging.warn("##################### Too large file %d, name: %s ################", f_size, abs_filename)

    dir_file = {
        'filename': os.path.basename(abs_filename),
        'sha256': sha256,
        'type': f_type,
        'extension': extension
    }

    file_info = {
        'stat_size': f_size,
        'stat_type': get_file_type(mode),
        'link': link
    }

    dir_file['file_info'] = file_info

    return dir_file

