
from imports import *
from draw_pic import *
from utility import *


def load_file(abs_filename):
    sha256 = hashlib.md5(open(abs_filename, 'rb').read()).hexdigest()

    f_size = os.lstat(abs_filename).st_size
    if f_size > 100000000000:
        logging.warn("Too large file %d, name: %s", f_size, abs_filename)

    f_type = me.from_file(abs_filename)
    extension = os.path.splitext(abs_filename)[1]

    dir_file = {
        # 'filename': abs_filename,
        'sha256': sha256,
        'size (B)': f_size,
        'type': f_type,
        'extension': extension
        # 'symlink': symlink,
        # 'statinfo': statinfo
    }

    return dir_file



# def compare_files(file1, file2):
#     md5 = hashlib.md5(open(file1, 'rb').read()).hexdigest()
#     sha256 = hashlib.sha256(open(file1, 'rb').read()).hexdigest()
#
#     md52 = hashlib.md5(open(file2, 'rb').read()).hexdigest()
#     sha2562 = hashlib.sha256(open(file2, 'rb').read()).hexdigest()
#
#     if md5 == md52 & sha256 == sha2562:
#         print "same files"
#         return 0
#     else:
#         print "not same files"
#         return 1

# def plt_files_size(images):
#     #files = []
#     sizes = []
#     #f_size = 0
#     for image in images:
#         for layer in image['layers']:
#             for dir in layer['dirs']:
#                 #for subdir in dir['subdir']:
#                 for f in dir['files']:
#                     if not os.path.isfile(os.path.join(dir['subdir'], f)):
#                         logging.info('no file found for this file: %s', f)
#                         continue
#
#                     f_size = os.lstat(os.path.join(dir['subdir'], f)).st_size
#                     if f_size > 100000000000:
#                         print (f_size, os.path.join(dir['subdir'], f))
#
#                     f_size = int(f_size) / 1024 / 1024
#
#                     sizes.append(f_size)
#
#     fig = fig_size('small')
#     #x = range(len(sizes))
#     y = sizes
#     #print y
#     print max(y)
#     xlim = 100
#     ticks = 50
#     #plot_cdf(fig, data, xlabel, xlim, ticks):
#     plot_cdf(fig, y, 'file size', xlim, ticks)



# def get_files_type(file_out, all_files):
#     f = open(file_out, 'w+')
#     for name in all_files:
#         line = magic.from_file(name)
#         f.writelines(line + '\n')
