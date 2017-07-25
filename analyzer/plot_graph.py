
# import cStringIO
from imports import *
from draw_pic import *
from utility import *
# from file import *

layer_base_info = {}
size = defaultdict(list)
dir_depth = defaultdict(list)
file_cnt = []

with open("saved_analyzed_layer_jsonfile.out", 'r') as f_analyzed_layer:
    json_datas = json.load(f_analyzed_layer)
    print json_datas

    for json_data in json_datas:
        size['uncompressed_sum_of_files'].append(json_data['uncompressed_sum_of_files'] / 1024 / 1024)
        size['compressed_size_with_method_gzip'].append(json_data['compressed_size_with_method_gzip'] / 1024 / 1024)
        size['archival_size'].append(json_data['archival_size'] / 1024 / 1024)

        dir_depth['dir_max_depth'].append(json_data['dir_max_depth'])

        file_cnt.append(json_data['file_cnt'])

layer_base_info['size'] = size
layer_base_info['dir_depth'] = dir_depth
layer_base_info['file_cnt'] = file_cnt

fig = fig_size('small')  # 'large'

data1 = layer_base_info['size']['uncompressed_sum_of_files']
xlabel = 'Uncompressed layer size (MB): sum of files'
xlim = mean(data1)  # max(data1)
ticks = 25
print xlim
plot_cdf(fig, data1, xlabel, xlim, ticks)
