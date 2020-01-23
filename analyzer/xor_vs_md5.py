from config import *
from dir import *
from layers import check_file_type
from itertools import izip

""" 
TODO

load input_f_for_xorVSmd5
dict by size.
use xor comparison
use md5 calculation

"""

class FileInfo:
    def __init__(self, size):
        self.size = size
        self.f_lst = []


def xor_vs_md5():
    manager = multiprocessing.Manager()
    files_dict = construct_file_info()

    logging.debug('Initializing the job lists....')

    files_dict_lst = []

    for fileinfo in (sorted(files_dict.values(), key=operator.attrgetter('size'))):
        files_dict_lst.append(fileinfo)

    ret_lst = process_file_lst(files_dict_lst)

    with open(output_f_for_xorVSmd5, 'w') as f:
        json.dump(ret_lst, f)


    logging.info('done! all the layer job processes are finished')


def process_file_lst(files_dict_lst):
    if not files_dict_lst:
        return None

    logging.info("create pool to do xor or md5 ....")
    P_cal = multiprocessing.Pool(60)

    logging.debug("map pool to decompress and unpack layer tarfiles ....")

    ret_lst = P_cal.map(xor_and_md5, files_dict_lst)

    logging.info("start close pool")
    P_cal.close()

    logging.info("start joinning workers....")
    P_cal.join()

    return ret_lst


def xor_and_md5(fileinfo):
    processname = multiprocessing.current_process().name

    size = fileinfo.size
    file_lst = fileinfo.f_lst

    logging.debug("[%s] process size: %d, cnt: %d", processname, size, len(file_lst))

    md5_cal_times = []
    xor_cmp_times = []
    sum_time_md5 = 0
    sum_time_xor = 0

    """do xor first"""

    start = time.time()
    for x, y in itertools.combinations(file_lst, 2):
        xor_cmp_times.append(xor(x, y, size))
    elapsed = time.time() - start

    sum_time_xor = elapsed

    start = time.time()
    for x in file_lst:
        md5_cal_times.append(md5(x, size))
    elapsed = time.time() - start

    sum_time_md5 = elapsed

    ret = {
        'md5_cal_times': md5_cal_times,
        'xor_cmp_times': xor_cmp_times,
        'size': size,
        'cnt': len(file_lst),
        'sum_time_md5': sum_time_md5,
        'sum_time_xor': sum_time_xor
    }

    if sum_time_md5 >= sum_time_xor:
        logging.info("=====> SUM xor WIN: size: %d, diff: %f", size, sum_time_md5 - sum_time_xor)
    else:
        logging.info("=====> SUM md5 WIN: size: %d, diff: %f", size, sum_time_xor - sum_time_md5)

    if sum_time_md5/len(file_lst) >= sum_time_xor/len(list(itertools.combinations(file_lst, 2))):
        logging.info("=====> MEAN xor WIN: size: %d, diff: %f", size, sum_time_md5/len(file_lst) - sum_time_xor/len(list(itertools.combinations(file_lst, 2))))
    else:
        logging.info("=====> MEAN md5 WIN: size: %d, diff: %f", size, sum_time_xor/len(list(itertools.combinations(file_lst, 2))) - sum_time_md5/len(file_lst))

    return ret


def xor(x, y, size):
    #######################
    # Powershell XOR 2 Files
    # xor.py
    # Jul 2016
    # Website: http://www.Megabeets.net
    # Use: ./xor.py file1 file2 outputFile
    # Example: ./xor.py C:\a.txt C:\b.txt C:\result.txt
    #######################

    start = time.time()

    if size > 1024*1024*1024: # bigger than 1GB
        logging.warn("##################### Too large file %d > 100000000000, name: %s ################", size, x)

        read_size = 1024 * 1024 * 1024  # You can make this bigger

        with open(x, 'rb') as f1, open(y, 'rb') as f2:
            data1 = bytearray(f1.read(read_size))
            data2 = bytearray(f2.read(read_size))
            sub_xor(data1, data2, read_size)
    else:
        file1_b = bytearray(open(x, 'rb').read())
        file2_b = bytearray(open(y, 'rb').read())
        sub_xor(file1_b, file2_b, size)

    elapsed = time.time() - start

    logging.debug("=====> xor: filename %s, %s, size: %d, elapsed: %f", x, y, size, elapsed)

    return elapsed


def sub_xor(x, y, read_size):
    # XOR between the bytearray
    for i in range(read_size):
        if x[i] ^ y[i] == 1:
            break
    return


def md5(x, size):

    start = time.time()

    if size > 1024*1024*1024: # bigger than 1GB
        logging.warn("##################### Too large file %d > 100000000000, name: %s ################", size, x)

        read_size = 1024 * 1024 * 1024  # You can make this bigger
        sha256 = hashlib.md5()
        with open(x, 'rb') as f:
            data = f.read(read_size)
            while data:
                sha256.update(data)
                data = f.read(read_size)
        sha256 = sha256.hexdigest()
    else:
        sha256 = hashlib.md5(open(x, 'rb').read()).hexdigest()

    elapsed = time.time() - start

    logging.debug("=====> md5: filename %s, size: %d, elapsed: %f", x, size, elapsed)

    return elapsed


def construct_file_info():
    logging.info('construct filename dict: a dict contains size and filename....')

    file_dict = {}
    with open(input_f_for_xorVSmd5) as f:
        for line in f:
            line = line.rstrip('\n')
            if os.path.isfile(line):
                size = os.lstat(line).st_size
                if size not in file_dict:
                    file_dict[size] = FileInfo(size)
                item = file_dict[size]
                item.f_lst.append(line)

    return file_dict
