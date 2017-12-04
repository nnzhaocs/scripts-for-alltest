
import os, time, logging, subprocess

"""python auto_download_compressed_images.py 
-f segments/segmentdn 
-d ~/2tb_hdd/downloaded/ 
-l ~/2tb_hdd/downloaded/downloader_list/downloaded-layers.lst 
-r ~/2tb_hdd/downloaded/downloader_list/downloaded-images.lst 
>> ~/2tb_hdd/downloaded/logs/downloader.11-29.log &
"""

downloaded_dirabsname = "/home/nannan/2tb_hdd/downloaded/"
downloaded_layers_fileabsname = "/home/nannan/2tb_hdd/downloaded/downloader_list/downloaded-layers.lst"
downloaded_images_fileabsname = "/home/nannan/2tb_hdd/downloaded/downloader_list/downloaded-images.lst"
#downloader_log_fileabsname = "/home/nannan/2tb_hdd/downloaded/logs/downloader"

input_files_dirabsname = "/home/nannan/2tb_hdd/segments/"

hdfs_layers_dirabsname = "/layers/"

thread_num = 10

input_files = []

def load_dir(dirname):
    for path, subdirs, files in os.walk(dirname):
        for f in os.listdir(path):
            filename = os.path.isfile(os.path.join(path, f))
            logging.debug("find a segment file: %s!", filename)
            input_files.append(filename)


def download_images(input_filename, downloaded_dirname, downloaded_layers_filename, downloaded_images_filename):
    start = time.time()
    cmd = 'python auto_download_compressed_images.py -f %s -d %s -l %s -r %s' \
          % (input_files, downloaded_dirname, downloaded_layers_filename, downloaded_images_filename)
    logging.debug('The shell command: %s', cmd)
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.debug('###################%s: %s###################',
                      input_filename, e.output)
        return False
    elapsed = time.time() - start
    logging.info('download compressed tarballs consumed time, ==> %f s', elapsed)
    return True

"""hadoop fs -copyFromLocal -t 20 <src_dirpath> /test"""

def copy_to_hdfs(thread_num, src_dirpath, hdfs_path):
    start = time.time()
    cmd = 'hadoop fs -copyFromLocal -t %d %s %s' \
          % (thread_num, src_dirpath, hdfs_path)
    logging.debug('The shell command: %s', cmd)
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.debug('###################%s: %s###################',
                      src_dirpath, e.output)
        return False
    elapsed = time.time() - start
    logging.info('copy to hdfs consumed time, ==> %f s', elapsed)
    return True


def mk_dir(dirname):
    cmd1 = 'mkdir %s' % dirname
    logging.debug('The shell command: %s', cmd1)
    try:
        subprocess.check_output(cmd1, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.debug('###################%s: %s###################',
                      dirname, e.output)
        # clear_dir(layer_dir)
        # subprocess.check_output(cmd1, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
        return False

    # elapsed = time.time() - start
    # logging.info('copya compressed tarball, ==> %f s', elapsed)
    return True


def hdfs_mk_dir(dirname):
    cmd1 = 'hadoop fs -mkdir %s' % dirname
    logging.debug('The shell command: %s', cmd1)
    try:
        subprocess.check_output(cmd1, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.debug('###################%s: %s###################',
                      dirname, e.output)
        # clear_dir(layer_dir)
        # subprocess.check_output(cmd1, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
        return False

    # elapsed = time.time() - start
    # logging.info('copya compressed tarball, ==> %f s', elapsed)
    return True


def main():
    fmt = "%(funcName)s():%(lineno)i: %(message)s %(levelname)s"
    logging.basicConfig(level=logging.DEBUG, format=fmt)

    load_dir(input_files_dirabsname)
    for f in input_files:
        logging.debug("=================> process file:%s <================", f)

        downloaded_dirname = os.path.join(downloaded_dirabsname, f)
        hdfs_dirname = os.path.join(hdfs_layers_dirabsname, f)

        logging.debug("downloaded_dirname: %s, hdfs_dirname: %s", downloaded_dirname, hdfs_dirname)

        if not mk_dir(downloaded_dirname):
            logging.debug("cannot create downloaded_dirname: %s", downloaded_dirname)
            continue
        if not hdfs_mk_dir(hdfs_dirname):
            logging.debug("cannot create hdfs_dirname: %s", hdfs_dirname)
            continue

        logging.debug("==================> START DOWNLOADING PROCESS <=================")
        download_images(f, downloaded_dirname, downloaded_layers_fileabsname, downloaded_images_fileabsname)

        logging.debug("==================> START COPYING TO HDFS <===================")
        copy_to_hdfs(thread_num, downloaded_dirname, hdfs_dirname)

        logging.debug("==================> finished file: %s <===================", f)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
