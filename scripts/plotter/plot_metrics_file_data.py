

from draw_pic import *

type = []
sha256 = []
stat_type = []
stat_size = []


def run_plotmetrics_image_data():
    load_image_metrics_data_file()
    try:
        plot_graph('type')
    except:
        print "Cannot plot type"
    try:
        plot_graph('sha256')
    except:
        print "cannot plot sha256"
    try:
        plot_graph('stat_type')
    except:
        print "cannot print stat_type"

    try:
        plot_graph('stat_size')
    except:
        print "cannot print stat_size"
    # try:
    #     plot_graph('archival_to_gzip_ratio')
    # except:
    #     print "cannot print archival_to_gzip_ratio"
    #
    # try:
    #     plot_graph('file_cnt')
    # except:
    #     print "cannot print file_cnt"
    # try:
    #     plot_graph('repeate_layer_digests')
    # except:
    #     print "cannot print repeate_layer_digests"


def plot_graph(type):

    print "===========> plot %s <=========="%type
    fig = fig_size('min')  # 'large'

    if type == 'type':
        data = type
        xlabel = 'File types'
        # data = [x * 1.0 / 1024 / 1024 for x in data1]
        xlim = int(mean(data))
    elif type == 'stat_type':
        data = stat_type
        xlabel = 'File stat type'
        # data = [x * 1.0 / 1024 / 1024 for x in data1]
        xlim = max(data)
    elif type == 'stat_size':
        data1 = stat_size
        xlabel = 'File size (KB)'
        data = [x * 1.0 / 1024 for x in data1]
        xlim = 250

    # elif type == 'sum_to_gzip_ratio':
    #     data = sum_to_gzip_ratio
    #     xlabel = 'Compression ratio: Uncompressed layer size as sum of files / compressed_size_with_method_gzip'
    #     xlim = max(data)
    # elif type == 'archival_to_gzip_ratio':
    #     data = archival_to_gzip_ratio
    #     xlabel = 'Compression ratio: Uncompressed layer tarball size / compressed_size_with_method_gzip'
    #     xlim = max(data)
    #
    # elif type == 'file_cnt':
    #     data = file_cnt
    #     xlabel = 'File count for each image'
    #     xlim = 300
    elif type == 'sha256':
        data = sha256
        xlabel = 'Repeate files across all images'
        xlim = max(data)

    print "mean = %f, len = %d"%(xlim, len(data))
    plot_cdf(fig, data, xlabel, xlim, 0)


def plot_repeates(type):
    print "===========> plot %s <=========="%type
    fig = fig_size('min')  # 'large'
    if type == 'repeate_layer_digests':
        data = repeate_layer_digests
        ylabel = 'Repeate layer count across all images'
        xlabel = 'Layer digest'

        lists = sorted(data.items(), key=lambda x: (-x[1], x[0]))  # sorted by value, return a list of tuples

        x, y = zip(*lists)

        xlim = len(x)

        ticks = 25

        plot_bar_pic(fig, x, y, xlabel, ylabel, xlim, ticks)


def load_image_metrics_data_file():

    with open(os.path.join(dest_dir[0]['job_list_dir'], 'image_metrics_datas.json'), 'r') as f_image_metrics_data:
        json_datas = json.load(f_image_metrics_data)

        for json_data in json_datas:
            uncompressed_sum_of_files.append(json_data['uncompressed_sum_of_files'])
            compressed_size_with_method_gzip.append(json_data['compressed_size_with_method_gzip'])
            archival_size.append(json_data['archival_size'])

            sum_to_gzip_ratio.append(json_data['sum_to_gzip_ratio'])
            archival_to_gzip_ratio.append(json_data['archival_to_gzip_ratio'])
            file_cnt.append(json_data['file_cnt'])

    with open(os.path.join(dest_dir[0]['job_list_dir'], 'repeate_layer_digests_dict.json'), 'r') as f_repeate_layer_digests_dict:
        json_datas = json.load(f_repeate_layer_digests_dict)
        for key, val in json_datas.items():
            repeate_layer_digests.append(val)

