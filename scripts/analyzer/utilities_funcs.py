
import sys
sys.path.append('../libraries/')
from config import *


def print_config():
    """configurations for setup"""

    print "dest_dirname = %s" % dest_dirname
    # print "extracting_dir = %s" % extracting_dir

    # print "analyzed_absfilename = %s" % analyzed_absfilename
    # print "layer_list_absfilename = %s" % layer_list_absfilename

    """configurations for multithreading"""

    print "num_worker_process = %d" % num_worker_process


def load_config():
    logging.info('!!! please put all the manifest files into the dest_dir/manifests directory!')
    print_config()
    logging.info('Input dir name: %s', dest_dirname)
    if not os.path.isdir(dest_dirname):
        logging.error('%s is not a valid dir', dest_dirname)
        return

    layer_dir = os.path.join(dest_dirname, "layers")
    if not os.path.isdir(layer_dir):
        logging.error('%s is not a valid dir', layer_dir)
        return

    manifest_dir = os.path.join(dest_dirname, 'manifests')
    if not os.path.isdir(manifest_dir):
        logging.error('%s is not a valid dir', manifest_dir)
        return

    logging.info('extracting_dir is: %s', extracting_dir)
    if not os.path.isdir(extracting_dir):
        logging.error('%s is not a valid file', extracting_dir)
        return

    job_list_dir = os.path.join(dest_dirname, 'job_list_dir')
    if not os.path.isdir(job_list_dir):
        logging.debug('make layer_db_json dir ==========> %s' % job_list_dir)
        cmd1 = 'mkdir %s' % job_list_dir
        logging.debug('The shell command: %s', cmd1)
        try:
            subprocess.check_output(cmd1, shell=True)
        except subprocess.CalledProcessError as e:
            print '###################' + e.output + '###################'
            return

    layer_db_json_dir = layer_db_json_dirname
    if not os.path.isdir(layer_db_json_dir):
        logging.debug('make layer_db_json dir ==========> %s' % layer_db_json_dir)
        cmd1 = 'mkdir %s' % layer_db_json_dir
        logging.debug('The shell command: %s', cmd1)
        try:
            subprocess.check_output(cmd1, shell=True)
        except subprocess.CalledProcessError as e:
            print '###################' + e.output + '###################'
            return

    image_db_json_dir = os.path.join(dest_dirname, 'image_db_json')
    if not os.path.isdir(image_db_json_dir):
        logging.debug('make image_db_json dir ==========> %s' % image_db_json_dir)
        cmd1 = 'mkdir %s' % image_db_json_dir
        logging.debug('The shell command: %s', cmd1)
        try:
            subprocess.check_output(cmd1, shell=True)
        except subprocess.CalledProcessError as e:
            print '###################' + e.output + '###################'
            return

    dir = {
        'dirname': dest_dirname,
        'manifest_dir': manifest_dir,
        #'config_dir': config_dir,
        'layer_dir': layer_dir,
        'extracting_dir': extracting_dir,
        'layer_db_json_dir': layer_db_json_dir,
        'image_db_json_dir': image_db_json_dir,
        'job_list_dir': job_list_dir
    }

    dest_dir.append(dir)
    logging.info('dest dir is: %s', dest_dir)


# def compress_tarball_tar(abs_gzip_file_name, layer_dir):
#     start = time.time()
#     # abs_zip_file_name = os.path.join(extracting_dir, layer_id + '-uncompressed-archival.tar')
#
#     cmd = 'tar -czf %s %s' % (abs_gzip_file_name, layer_dir)
#     logging.debug('The shell command: %s', cmd)
#     try:
#         subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
#     except subprocess.CalledProcessError as e:
#         logging.error('###################%s: exit code: %s; %s###################',
#                       abs_gzip_file_name, e.returncode, e.output)
#         return False
#
#     elapsed = time.time() - start
#     logging.info('tar gzip directory, consumed time ==> %f s', elapsed)
#     return True

# def decompress_tarball_tar(cp_layer_tarball_name, layer_dir):
#     start = time.time()
#     cmd = 'tar -pzxf %s -C %s' % (cp_layer_tarball_name, layer_dir)
#     logging.debug('The shell command: %s', cmd)
#     try:
#         subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
#     except subprocess.CalledProcessError as e:
#         logging.debug('###################%s: exit code: %s; %s###################',
#                       cp_layer_tarball_name, e.returncode, e.output)
#         return False
#
#     elapsed = time.time() - start
#     logging.info('process layer_id:%s : tar decompress and extract tarball, consumed time ==> %f s', elapsed)
#     logging.debug('FINISHED! to ==========> %s', layer_dir)
#     return True