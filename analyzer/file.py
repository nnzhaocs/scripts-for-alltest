
from imports import *
from draw_pic import *
from utility import *


def get_file_type(mode):
    if S_ISREG(mode):  # tarfile.DIRTYPE
        return 'REGTYPE'
    elif S_ISLNK(mode):  # tarfile.DIRTYPE
        return 'SYMTYPE'
    elif S_ISFIFO(mode):  # tarfile.DIRTYPE
        return 'FIFOTYPE'
    elif S_ISCHR(mode):  # tarfile.DIRTYPE
        return 'CHRTYPE'
    elif S_ISBLK(mode):  # tarfile.DIRTYPE
        return 'BLKTYPE'
    elif S_ISSOCK(mode):
        return 'SOCK'
    else:
        return 'Unknown'


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

    elif stat.st_nlink >= 1:
        link = {
            'link_type': 'hardlink',
            'num_hdlinks': stat.st_nlink
        }

    elif S_ISREG(mode):
        f_size = os.lstat(abs_filename).st_size
        if f_size > 1000000000:
            logging.warn("##################### Too large file %d, name: %s ################", f_size, abs_filename)

        try:
            sha256 = hashlib.md5(open(abs_filename, 'rb').read()).hexdigest()
        except MemoryError as e:
            logging.debug("##################### Memory Error #####################: %s", e)
            logging.debug("##################### Too large file %d, name: %s ################", f_size, abs_filename)
            read_size = 1024  # You can make this bigger
            sha256 = hashlib.md5()
            with open(abs_filename, 'rb') as f:
                data = f.read(read_size)
                while data:
                    sha256.update(data)
                    data = f.read(read_size)
            sha256 = sha256.hexdigest()

        f_type = me.from_file(abs_filename)
        extension = os.path.splitext(abs_filename)[1]

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

