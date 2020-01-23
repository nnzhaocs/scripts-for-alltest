import sys
sys.path.append('../libraries/')
from regular_libraries import *
import psutil
import shlex

"""configurations for setup"""

src_dir = "/home/nannan/dockerimages/layers" #"/home/nannan/2tb_hdd"
dest_dir = os.path.join(src_dir, "tmp_dir")
src_file_lst = ["hulk1_layers_less_50m.lst",
                "hulk1_layers_less_1g.lst",
                "hulk1_layers_less_2g.lst",
                "hulk1_layers_bigger_2g.lst"]

TIMEOUT = 3 #3s
Threadnum = 1
###################################

def cp_file(src, dest):

    cmd = 'cp %s  %s' % (src, dest)
    logging.debug('The shell command: %s', cmd)
    try:
        subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.error('###################%s: %s###################',
                      src, e.output)
        return False
    return True


def clear_dir(dest):
    """ delete the dir """
    if not os.path.isdir(dest):
        logging.error('###################%s is not valid###################', dest)
        return False

    cmd4 = 'rm -rf %s' % (dest+'*')
    logging.debug('The shell command: %s', cmd4)
    try:
        subprocess.check_output(cmd4, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.error('###################%s: exit code: %s; %s###################',
                      dest, e.returncode, e.output)
        return False

    return True

def mk_dir(dest):
    #command = 'ls -l {}'.format(quote(filename))
    cmd1 = 'mkdir -pv {}'.format(quote(dest))
    logging.debug('The shell command: %s', cmd1)
    try:
        subprocess.check_output(cmd1, stderr=subprocess.STDOUT, shell=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        logging.error('###################%s: %s###################',
                      dest, e.output)
        return False
    return True


def queue_layers(input_fname):
    num = 0
    with open(input_fname) as f:
        for line in f:
            print line
            #num = num + 1
            #if num > 50:
            #break
            if line:
                logging.debug('queue layer_id: %s to lst', line.replace("\n", ""))  #
                lst.append(line.replace("\n", ""))
    
    return lst

#########################################

def get_memory_usage(p):
#     p = psutil.Process()
    return p.memory_percent()

def get_cpu_load(p):
#     p = psutil.Process()
    return p.cpu_percent() # non-blocking (percentage since last call)
    
def get_disk_io_counters(p):
#     p = psutil.Process()
    return p.io_counters().as_dict(p)
    
def get_process_resource_utilization(p):
    
    pio = get_disk_io_counters(p)
    
    resource_utilization = (get_cpu_load(p), get_memory_usage(p), pio.read_chars, pio.write_chars)
    
    return resource_utilization

    
def spwan_subprocess(cmd):
    
    resource_utilization =()
    
    try:
        start = time.time()
        with psutil.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE) as p:
            print p.stdout.read()
            resource_utilization = get_process_resource_utilization(p)
        elapsed = time.time() - start
        
        outs, errs = p.communicate()  
        if errs:
            logging.debug('################### [%s]: OUTPUTS: %s: STDERRS: %s ###################',
                      cmd, outs, errs)
        if 0 == p.poll():
            logging.debug('Finished [%s]', cmd)       
    except Exception:
        logging.debug('################### [%s]: OUTPUTS: %s: STDERRS: %s ###################',
                      cmd, outs, errs)
        
        return resource_utilization + (-1,)
    
    return resource_utilization + (elapsed,)
        
##########################################

'''
    output format:
        sha256 \t size \t decompressed size \t compress time \t decompress time \t pack time \t unpack time \t
        compress cpu usage \t decompressed cpu usage \t compress memory usage \t decompressed memory usage \t
        pack cpu usage \t unpack cpu usage \t pack memory usage \t unpack memory usage \t
'''


def pack(src, dest): # directory
    
    cmd = 'tar -cf %s %s' % (dest, src)
    logging.debug('The shell command: %s', cmd)
    
    resuts = spwan_subprocess(cmd)

    return results


def unpack(src, dest):

    cmd = 'tar -pxf %s -C %s' % (src, dest)
    logging.debug('The shell command: %s', cmd)
    
    resuts = spwan_subprocess(cmd)
    
    return results


def gunzip(src, dest):

    cmd = 'gunzip -c %s > %s' % (src, dest)
    logging.debug('The shell command: %s', cmd)
    
    resuts = spwan_subprocess(cmd)
    
    return results


def gzip(src): #.gz

    cmd = 'gzip -f %s' % (src)
    logging.debug('The shell command: %s', cmd)
    
    resuts = spwan_subprocess(cmd)
    
    return results


def process_layers(abslfname):
    
    results = []
    
    uncompressed_archival_size = -1
    compressed_archival_size = -1

    if os.path.isfile(abslfname):
        compressed_archival_size = os.lstat(abslfname).st_size
        logging.debug("compressed_archival_size %d B, name: %s", compressed_archival_size, abslfname)
    else:
        return
    
    layer_filename = os.path.basename(abslfname)

    extracting_dir = dest_dir
    
    layer_dir = os.path.join(extracting_dir, layer_filename) # layer working dir
    
    mk_dir(layer_dir)
    
    cp_layer_tarball_name = os.path.join(layer_dir, layer_filename + '-cp.tar.gz')
    
    archival_file_name = os.path.join(layer_dir, layer_filename + '-archival.tar')
#     abs_zip_file_name = os.path.join(extracting_dir, layer_filename + '-uncompressed-archival.tar')
    
    unpack_dir = os.path.join(extracting_dir, layer_filename + '-unpack_dir')
    mk_dir(unpack_dir)
    
    new_archival_fname = os.path.join(extracting_dir, layer_filename + '-new-archival.tar')
#     abs_gzip_file_name = abs_zip_file_name + '.gz'
    
    """cp"""
    
    if not cp_file(abslfname, cp_layer_tarball_name):
        clear_dir(layer_dir)
        return

    if not os.path.isfile(cp_layer_tarball_name):
        logging.warn('cp layer tarball file %s is invalid', cp_layer_tarball_name)
        clear_dir(layer_dir)
        return 
    
    """uncompress"""
    gunzip_results = gunzip(cp_layer_tarball_name, archival_file_name) # gunzip decompression

    if os.path.isfile(archival_file_name):
        uncompressed_archival_size = os.lstat(archival_file_name).st_size
        logging.debug("uncompressed_archival_size %d B, name: %s", uncompressed_archival_size, layer_dir)
    else:
        logging.debug("uncompressed_archival_wrong!!! name: %s", layer_dir)
        clear_dir(layer_dir)
        return
    
    """unpack"""
    unpack_results = unpack(archival_file_name, unpack_dir)

    if not os.path.isdir(unpack_dir):
        logging.warn('layer dir %s is invalid', unpack_dir)
        clear_dir(layer_dir)
        return

    """pack"""
    
    pack_results = pack(unpack_dir, new_archival_fname)

    if os.path.isfile(new_archival_fname):
        pass
    else:
        logging.debug("pack_archival_wrong!!! name: %s", layer_dir)
        clear_dir(layer_dir)
        return
    
    """compress"""
    
    gzip_results = gzip(new_archival_fname)
    
    if not os.path.isfile(archival_file_name+'.gz'):
        clear_dir(layer_dir)
        return

    """clear working dir"""
    
    clear_dir(layer_dir)

    results.append(gunzip_results, unpack_results, pack_results, gzip_results, uncompressed_archival_size, compressed_archival_size)
    return results

def main():
    
    fmt="%(funcName)s():%(lineno)i: %(message)s %(levelname)s"
    logging.basicConfig(level=logging.INFO, format=fmt) #logging.DEBUG
    
    fp = os.open('results.lst', 'w')
#     p = Pool(Threadnum)

    logging.info('Logging app started')

    for lst in src_file_lst:
        logging.debug('process lst: %s', lst)
        layers = queue_layers(os.path.join(src,lst))
        for layer in layers:
            results = process_layers(layer)
            
            fp.write(results + '\n')
        logging.info("lst: %s successfully finished.", lst)
#         p.map(process_layers, layers)

#        break

    


if __name__ == "__main__":
    main()