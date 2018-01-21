import sys

sys.path.append('../plotter/')
from draw_pic import *
#from analysis_library import *
import pandas as pd
import pandas

RESULTS_DIR = '/home/nannan/4tb_results'

#draw_type_by_repeat_cnt = os.path.join(RESULTS_DIR, 'draw_type_by_repeat_cnt.csv')
#draw_type_by_total_sum = os.path.join(RESULTS_DIR, 'draw_type_by_total_sum.csv')
#draw_type1_by_repeat_cnt = os.path.join(RESULTS_DIR, 'draw_type1_by_repeat_cnt.csv')
#draw_type_by_size = os.path.join(RESULTS_DIR, 'draw_type_by_size.csv')
#draw_type_by_dup_ratio_cap = os.path.join(RESULTS_DIR, 'draw_type_by_dup_ratio_cap.csv')
#draw_type_by_dup_ratio_cnt = os.path.join(RESULTS_DIR, 'draw_type_by_dup_ratio_cnt.csv')

#capacity_data = os.path.join(RESULTS_DIR, 'capacity_data.csv')

draw_size_uniq = os.path.join(RESULTS_DIR, 'draw_size_uniq_data.csv')
draw_size_shared = os.path.join(RESULTS_DIR, 'draw_size_shared_data.csv')
draw_size_whole = os.path.join(RESULTS_DIR, 'draw_size_whole_data.csv')
draw_repeat_cnt = os.path.join(RESULTS_DIR, 'draw_repeat_cnt_data.csv')

tmp = os.path.join(RESULTS_DIR, 'draw_layer_shared_cnt_data.csv')


def main():
    #draw_file_repeat_cnt()
    draw_file_size()


def draw_3_data_cdf(fig, data1, data2, data3, xlabel, ylabel_cdf):
    ax = fig.add_subplot(111)

    bins_size_data1 = np.arange(data1.min()-1, data1.max()+1)
    bins_size_data2 = np.arange(data2.min()-1, data2.max()+1)
    bins_size_data3 = np.arange(data3.min()-1, data3.max()+1)

    print "cdf and pdf calculating: bins = %d, %d, %d" % (len(bins_size_data1), len(bins_size_data2), len(bins_size_data3))
    counts_cdf_size_data1, base_cdf_size_data1 = np.histogram(data1, bins=bins_size_data1, normed=True)
    counts_cdf_size_data2, base_cdf_size_data2 = np.histogram(data2, bins=bins_size_data2, normed=True)
    counts_cdf_size_data3, base_cdf_size_data3 = np.histogram(data3, bins=bins_size_data3, normed=True)

    cdf_size_data1 = np.cumsum(counts_cdf_size_data1)
    cdf_size_data2 = np.cumsum(counts_cdf_size_data2)
    cdf_size_data3 = np.cumsum(counts_cdf_size_data3)

    print "start plotting!"

    line_data1 = ax.semilogx(base_cdf_size_data1[1:], cdf_size_data1, 'b-', linewidth=2, label='Repeat cnt. = 1')
    line_data2 = ax.semilogx(base_cdf_size_data2[1:], cdf_size_data2, 'r-', linewidth=2, label='Repeat cnt. > 1')
    line_data3 = ax.semilogx(base_cdf_size_data3[1:], cdf_size_data3, 'g-', linewidth=2, label='Whole')

    plt.legend(cd + pd, [l.get_label() for l in (line_data1 + line_data2 + line_data3)], loc='center right', prop={'size': 18})

    print "start labeling!"
    ax.set_xlim(2, counts_cdf_size_data3.max())
    ax.set_ylim(0, 1)

    ax.set_xlabel(xlabel, fontsize=18)
    ax.set_ylabel(ylabel_cdf, fontsize=18)  # 24,>14
    ax.get_yaxis().set_tick_params(labelsize=18)
    ax.get_xaxis().set_tick_params(labelsize=18)

    plt.grid()
    plt.tight_layout()
    name = '%s_cdf.png' % xlabel.replace(" ", "_")#.replace("/","divided_by").replace(":","")
    fig.savefig(name)
    name = '%s_cdf.eps' % xlabel.replace(" ", "_")
    fig.savefig(name)


def draw_3_data_pdf(fig, data1, data2, data3, xlabel, ylabel_pdf):
    ax = fig.add_subplot(111)

    bins_size_data1 = np.arange(data1.min()-1, data1.max()+1)
    bins_size_data2 = np.arange(data2.min()-1, data2.max()+1)
    bins_size_data3 = np.arange(data3.min()-1, data3.max()+1)

    print "cdf and pdf calculating: bins = %d, %d, %d" % (len(bins_size_data1), len(bins_size_data2), len(bins_size_data3))
    counts_cdf_size_data1, base_cdf_size_data1 = np.histogram(data1, bins=bins_size_data1, normed=True)
    counts_cdf_size_data2, base_cdf_size_data2 = np.histogram(data2, bins=bins_size_data2, normed=True)
    counts_cdf_size_data3, base_cdf_size_data3 = np.histogram(data3, bins=bins_size_data3, normed=True)

    print "start plotting!"

    line_data1 = ax.semilogx(base_cdf_size_data1[1:], counts_cdf_size_data1, 'b-', linewidth=2, label='Repeat cnt. = 1')
    line_data2 = ax.semilogx(base_cdf_size_data2[1:], counts_cdf_size_data2, 'r-', linewidth=2, label='Repeat cnt. > 1')
    line_data3 = ax.semilogx(base_cdf_size_data3[1:], counts_cdf_size_data3, 'g-', linewidth=2, label='Whole')

    plt.legend(cd + pd, [l.get_label() for l in (line_data1 + line_data2 + line_data3)], loc='center right', prop={'size': 18})

    print "start labeling!"
    ax.set_xlim(2, counts_cdf_size_data3.max())
    ax.set_ylim(0, 1)

    ax.set_xlabel(xlabel, fontsize=18)
    ax.set_ylabel(ylabel_pdf, fontsize=18)  # 24,>14
    ax.get_yaxis().set_tick_params(labelsize=18)
    ax.get_xaxis().set_tick_params(labelsize=18)

    plt.grid()
    plt.tight_layout()
    name = '%s_pdf.png' % xlabel.replace(" ", "_")#.replace("/","divided_by").replace(":","")
    fig.savefig(name)
    name = '%s_pdf.eps' % xlabel.replace(" ", "_")
    fig.savefig(name)


def draw_file_size():
    size_uniq = pd.read_csv(draw_size_uniq)
    size_shared = pd.read_csv(draw_size_shared)
    size_whole = pd.read_csv(draw_size_whole)

    print("after loading file!")
    data_size_uniq = size_uniq.as_matrix()/1024.0
    data_size_shared = size_shared.as_matrix()/1024.0
    data_size_whole = size_whole.as_matrix()/1024.0

    ylabel_cdf = 'Cumulative file probability'
    xlabel = 'File size(KB)'

    fig = fig_size('min')

    draw_3_data_cdf(fig, data_size_uniq, data_size_shared, data_size_whole, xlabel, ylabel_cdf)
    
    """plot pdf"""

    ylabel_pdf = 'File probability'
    xlabel = 'File size(KB)'

    draw_3_data_cdf(fig, data_size_uniq, data_size_shared, data_size_whole, xlabel, ylabel_pdf)


"""plot two lines: pdf and cdf for a single data"""
def draw_1_data_cdf_and_pdf_same_graph(fig, data1, xlabel, ylabel_cdf, ylabel_pdf):
    data = data1
    ax = fig.add_subplot(111)

    print("plot: min = %d" % data.min())
    print("plot: max = %d" % data.max())
    print("plot: median = %d" % np.median(data))

    bins = np.arange(data.min() - 1, data.max() + 1)
    print "cdf and pdf calculating: bins = %d" % len(bins)

    counts_cdf, base_cdf = np.histogram(data, bins=bins, normed=True)
    cdf = np.cumsum(counts_cdf)

    counts_pdf, base_pdf = np.histogram(data, bins=bins, normed=True)

    print "start plotting!"

    cd = ax.semilogx(base_cdf[1:], cdf, 'b-', linewidth=2, label='Cumulative distribution')
    ax2 = ax.twinx()
    pd = ax2.semilogx(base_pdf[1:], counts_pdf, 'r-', linewidth=2, label='Probability distribution')

    print "start labeling!"
    ax.set_xlim(2, data.max())
    ax.set_ylim(0, 1)
    ax2.set_ylim(0, 1)

    ax.set_xlabel(xlabel, fontsize=18)
    ax.set_ylabel(ylabel_cdf, fontsize=18)
    ax2.set_ylabel(ylabel_pdf, fontsize=18)

    ax.get_xaxis().set_tick_params(labelsize=18)
    ax.get_yaxis().set_tick_params(labelsize=18)
    ax2.get_yaxis().set_tick_params(labelsize=18)

    plt.legend(cd+pd, [l.get_label() for l in (cd+pd)], loc='center right', prop={'size':18})
    plt.grid()
    plt.tight_layout()
    name = '%s.png' % xlabel.replace(" ", "_")#.replace("/","divided_by").replace(":","")
    fig.savefig(name)
    name = '%s.eps' % xlabel.replace(" ", "_")
    fig.savefig(name)


def draw_file_repeat_cnt():
    df = pandas.read_csv(draw_repeat_cnt)
    print("after loading file!")
    data = df.as_matrix()
    print(data)
    ylabel_cdf = 'Cumulative file probability'
    ylabel_pdf = 'File probability'
    xlabel = 'File repeat count'

    fig = fig_size('min')

    draw_1_data_cdf_and_pdf_same_graph(fig, data, xlabel, ylabel_cdf, ylabel_pdf)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
