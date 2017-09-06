

from draw_pic import *

uncompressed_sum_of_files = []
compressed_size_with_method_gzip = []
archival_size = []
sum_to_gzip_ratio = []
archival_to_gzip_ratio = []
file_cnt = []

dir_max_depth = []
dir_min_depth = []
dir_median_depth = []
dir_avg_depth = []

dir_cnt = []


def run_plotmetrics_layer_data():
    load_image_metrics_data_file()
    try:
        plot_graph('uncompressed_sum_of_files')
    except:
        print "Cannot plot uncompressed_sum_of_files"
    try:
        plot_graph('compressed_size_with_method_gzip')
    except:
        print "cannot plot compressed_size_with_method_gzip"
    try:
        plot_graph('archival_size')
    except:
        print "cannot print archival_size"

    try:
        plot_graph('sum_to_gzip_ratio')
    except:
        print "cannot print sum_to_gzip_ratio"
    try:
        plot_graph('archival_to_gzip_ratio')
    except:
        print "cannot print archival_to_gzip_ratio"

    try:
        plot_graph('file_cnt')
    except:
        print "cannot print file_cnt"
    try:
        plot_graph('dir_cnt')
    except:
        print "cannot print dir_cnt"

    try:
        plot_graph('dir_max_depth')
    except:
        print "cannot print dir_max_depth"


def plot_graph(type):

    print "===========> plot %s <=========="%type
    fig = fig_size('min')  # 'large'

    if type == 'uncompressed_sum_of_files':
        data1 = uncompressed_sum_of_files
        xlabel = 'Uncompressed layer size (MB) as sum of files'
        data = [x * 1.0 / 1024 / 1024 for x in data1]
        xlim = int(mean(data))
    elif type == 'compressed_size_with_method_gzip (MB)':
        data1 = compressed_size_with_method_gzip
        xlabel = 'Compressed layer tarball size (MB)'
        data = [x * 1.0 / 1024 / 1024 for x in data1]
        xlim = max(data)
    elif type == 'archival_size':
        data1 = archival_size
        xlabel = 'Uncompressed layer tarball size (MB)'
        data = [x * 1.0 / 1024 / 1024 for x in data1]
        xlim = 250

    elif type == 'sum_to_gzip_ratio':
        data = sum_to_gzip_ratio
        xlabel = 'Compression ratio: Uncompressed layer size as sum of files / compressed_size_with_method_gzip'
        xlim = max(data)
    elif type == 'archival_to_gzip_ratio':
        data = archival_to_gzip_ratio
        xlabel = 'Compression ratio: Uncompressed layer tarball size / compressed_size_with_method_gzip'
        xlim = max(data)

    elif type == 'file_cnt':
        data = file_cnt
        xlabel = 'File count for each image'
        xlim = 300
    elif type == 'dir_cnt':
        data = dir_cnt
        xlabel = 'Layer directory count across all images'
        xlim = max(data)
    elif type == 'dir_max_depth':
        data = dir_max_depth
        xlabel = 'Layer directory depth for each image'
        xlim = max(data)

    print "xlim = %f, len = %d"%(xlim, len(data))
    plot_cdf(fig, data, xlabel, xlim, 0)


def load_image_metrics_data_file():

    with open(os.path.join(dest_dir[0]['job_list_dir'], 'layer_metrics_datas.json'), 'r') as f_layer_metrics_datas:
        _json_datas = json.load(f_layer_metrics_datas)

        json_datas = list(chain(*_json_datas))

        print json_datas[0]

        for json_data in json_datas:
            uncompressed_sum_of_files.append(json_data['uncompressed_sum_of_files'])
            compressed_size_with_method_gzip.append(json_data['compressed_size_with_method_gzip'])
            archival_size.append(json_data['archival_size'])

            sum_to_gzip_ratio.append(json_data['sum_to_gzip_ratio'])
            archival_to_gzip_ratio.append(json_data['archival_to_gzip_ratio'])
            file_cnt.append(json_data['file_cnt'])

            # dir_max_depth = []
            # dir_min_depth = []
            # dir_median_depth = []
            # dir_avg_depth = []

            dir_cnt.append(json_data['dir_cnt'])

