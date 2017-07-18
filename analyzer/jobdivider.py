
from imports import *
from utility import *


def create_job_list():
    job_list_dir = dest_dir[0]['job_list_dir']

    tarballs = {}

    for path, _, tarball_filenames in os.walk(dest_dir[0]['layer_dir']):
        for tarball_filename in tarball_filenames:
            f_size = os.lstat(os.path.join(path, tarball_filename)).st_size
            tarballs[tarball_filename] = f_size
            logging.debug('layer_tarball: %s, size %d', tarball_filename, f_size)  # str(layer_id).replace("/", "")
            # logging.debug('queue dir layer tarball: %s', tarball_filename)  # str(layer_id).replace("/", "")
    #d_ascending = OrderedDict(sorted(tarballs.items(), key=lambda kv: kv[1]))
    #logging.debug(d_ascending)

    list_50mb = os.path.join(job_list_dir, 'list_less_50m.out')
    with open(list_50mb, 'w+') as f_out:
        tmp_dict = {key: val for key, val in tarballs.items() if val <= 50*1024*1024}
        json.dump(tmp_dict, f_out)

    list_1gb = os.path.join(job_list_dir, 'list_less_1g.out')
    with open(list_1gb, 'w+') as f_out:
        tmp_dict = {key: val for key, val in tarballs.items() if val > 50*1024*1024 and val <= 1024*1024*1024}
        json.dump(tmp_dict, f_out)

    list_2gb = os.path.join(job_list_dir, 'list_less_2g.out')
    with open(list_2gb, 'w+') as f_out:
        tmp_dict = {key: val for key, val in tarballs.items() if val > 1024*1024*1024  and val <= 2*1024*1024*1024}
        json.dump(tmp_dict, f_out)

    list_b_2gb = os.path.join(job_list_dir, 'list_bigger_2g.out')
    with open(list_b_2gb, 'w+') as f_out:
        tmp_dict = {key: val for key, val in tarballs.items() if val > 2*1024*1024*1024}
        json.dump(tmp_dict, f_out)
