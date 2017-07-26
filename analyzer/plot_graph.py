

# import cStringIO
from imports import *
from draw_pic import *
from utility import *
# from file import *

layer_base_info = {}
size = defaultdict(list)
dir_depth = defaultdict(list)
file_cnt = []


def plot_all(args):
    load_file()
    try:
        plot_graph('compressed_size_with_method_gzip')
    except:
        print "Cannot plot compressed_size_with_method_gzip"
    try:
        plot_graph('archival_size')
    except:
        print "cannot plot archival_size"
    try:
        plot_graph('dir_max_depth')
    except:
        print "cannot print dir_max_depth"
    try:
        plot_graph('file_cnt')
    except:
        print "cannot print file_cnt"


def plot_graph(type):
    #load_file()
    print "===========plot %s"%type
    fig = fig_size('small')  # 'large'

    if type == 'uncompressed_sum_of_files':
        data1 = layer_base_info['size']['uncompressed_sum_of_files']
        xlabel = 'Uncompressed layer size (MB): sum of files'
	data = [x * 1.0 / 1024 / 1024 for x in data1]
    elif type == 'compressed_size_with_method_gzip':
        data1 = layer_base_info['size']['compressed_size_with_method_gzip']
        xlabel = 'compressed_size_with_method_gzip (MB)'
	data = [x * 1.0 / 1024 / 1024 for x in data1]
	xlim = max(data)
    elif type == 'archival_size':
        data1 = layer_base_info['size']['archival_size']
        xlabel = 'archival_size (MB)'
	data = [x * 1.0 / 1024 / 1024 for x in data1]
	xlim = 2000
    elif type == 'dir_max_depth':
        data = layer_base_info['dir_depth']['dir_max_depth']
        xlabel = 'dir_max_depth'
	xlim = max(data)
    elif type == 'file_cnt':
        data = layer_base_info['file_cnt']
        xlabel = 'file_cnt'
	xlim = 2000

    #data = [x * 1.0 / 1024 / 1024 for x in data1]
    #xlim = int(mean(data))  # max(data1)
    ticks = 25
    print "mean = %f, len = %d"%(xlim,len(data))
    plot_cdf(fig, data, xlabel, xlim, ticks)


def load_file():
    with open("saved_analyzed_layer_jsonfile.out", 'r') as f_analyzed_layer:
        json_datas = json.load(f_analyzed_layer)
        #print json_datas

        for json_data in json_datas:
            size['uncompressed_sum_of_files'].append(json_data['uncompressed_sum_of_files'])
            size['compressed_size_with_method_gzip'].append(json_data['compressed_size_with_method_gzip'])
            size['archival_size'].append(json_data['archival_size'])

            dir_depth['dir_max_depth'].append(json_data['dir_max_depth'])

            file_cnt.append(json_data['file_cnt'])

    layer_base_info['size'] = size
    layer_base_info['dir_depth'] = dir_depth
    layer_base_info['file_cnt'] = file_cnt


