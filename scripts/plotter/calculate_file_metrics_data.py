
from algorithm_funcs import *
dest_dirname = "/home/nannan/2tb_hdd/"

def load_file_metrics_data_files():
    types = ['type', 'sha256', 'stat_type']
    #defaultdict(list)
    print types
    for type in types:
        file_metrics_datas = []
	print "load file"+os.path.join(dest_dirname, 'job_list_dir/file_metrics_datas_%s.json' % type)

        with open(os.path.join(dest_dirname, 'job_list_dir/file_metrics_datas_%s.json' % type),
                  'r') as f_layer_metrics_datas:
            for line in f_layer_metrics_datas:
                if type == 'type':
                    file_metrics_datas.append(line.split()[0])
                else:
                    file_metrics_datas.append(line)
            # file_metrics_datas = json.load(f_layer_metrics_datas)
        calaculate_file_metrics(file_metrics_datas, type)
        del file_metrics_datas

def calaculate_file_metrics(file_metrics_datas, type):
    """get repeat files"""

    file_dict = calculate_repeates(file_metrics_datas)
    print "write to"+os.path.join(dest_dirname, 'job_list_dir/repeate_file_%s.json'%type)
    with open(os.path.join(dest_dirname, 'job_list_dir/repeate_file_%s.json'%type), 'w') as f:
        json.dump(file_dict, f)


def main():
    load_file_metrics_data_files()


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
