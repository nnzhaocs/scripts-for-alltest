
from algorithm_funcs import *




        # with open(os.path.join(dest_dir[0]['job_list_dir'], 'layer_metrics_datas_%s.json'%type), 'w+') as f_layer_metrics_datas:
        #     json.dump(file_metrics_datas, f_layer_metrics_datas)
        #
        # calaculate_file_metrics(file_metrics_datas, type)
        #
        # del file_metrics_datas


# def load_file_metrics_data_files():
#     types = ['type', 'sha256', 'stat_type', 'stat_size']
#     for type in types:
#         with open(os.path.join(dest_dir[0]['job_list_dir'], 'layer_metrics_datas_%s.json' % type),
#                   'r') as f_layer_metrics_datas:
#             file_metrics_datas = json.load(f_layer_metrics_datas)
#         calaculate_file_metrics(file_metrics_datas, type)