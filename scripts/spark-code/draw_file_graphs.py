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


def main():
    #draw_file_repeat_cnt()
    draw_file_size()


def draw_file_size():
    size_uniq = pd.read_csv(draw_size_uniq)
    size_shared = pd.read_csv(draw_size_shared)
    size_whole = pd.read_csv(draw_size_whole)

    print("after loading file!")
    data_size_uniq = size_uniq.as_matrix()/1024.0
    data_size_shared = size_shared.as_matrix()/1024.0
    data_size_whole = size_whole.as_matrix()/1024.0

    ylabel = 'Cumulative file probability'
    xlabel = 'File size(B)'

    fig = fig_size('min')

    ax = fig.add_subplot(111)

    bins_size_uniq = np.arange(np.ceil(data_size_uniq.min()), np.floor(data_size_uniq.max()))
    bins_size_shared = np.arange(np.ceil(data_size_shared.min()), np.floor(data_size_shared.max()))
    bins_size_whole = np.arange(np.ceil(data_size_whole.min()), np.floor(data_size_whole.max()))

    print "cdf and pdf calculating: bins = %d, %d, %d" % (len(bins_size_uniq), len(bins_size_shared), len(bins_size_whole))
    counts_cdf_size_uniq, base_cdf_size_uniq = np.histogram(data_size_uniq, bins=bins_size_uniq, normed=True)
    counts_cdf_size_shared, base_cdf_size_shared = np.histogram(data_size_shared, bins=bins_size_shared, normed=True)
    counts_cdf_size_whole, base_cdf_size_whole = np.histogram(data_size_whole, bins=bins_size_whole, normed=True)

    cdf_size_uniq = np.cumsum(counts_cdf_size_uniq)
    cdf_size_shared = np.cumsum(counts_cdf_size_shared)
    cdf_size_whole = np.cumsum(counts_cdf_size_whole)

    print "start plotting!"

    cd = plt.semilogx(base_cdf_size_uniq[1:], cdf_size_uniq, 'b-', linewidth=1, label='Repeat cnt. = 1')
    cd = plt.semilogx(base_cdf_size_shared[1:], cdf_size_shared, 'b-', linewidth=1, label='Repeat cnt. > 1')
    cd = plt.semilogx(base_cdf_size_whole[1:], cdf_size_whole, 'b-', linewidth=1, label='Whole')

    print "start labeling!"

    ax.set_ylim(0, 1)

    ax.set_xlabel(xlabel, fontsize=18)
    ax.set_ylabel(ylabel, fontsize=18)  # 24,>14
    ax.get_yaxis().set_tick_params(labelsize=18)
    ax.get_xaxis().set_tick_params(labelsize=18)

    plt.grid()
    plt.tight_layout()
    name = 'file_size_cdf.png'
    fig.savefig(name)
    eps = 'file_size_cdf.eps'
    fig.savefig(eps)

    """plot pdf"""

    ylabel = 'File frequency'
    xlabel = 'File size(B)'

    counts_pdf_size_uniq, base_pdf_size_uniq = np.histogram(data_size_uniq, bins=bins_size_uniq, normed=False)
    counts_pdf_size_shared, base_pdf_size_shared = np.histogram(data_size_shared, bins=bins_size_shared, normed=False)
    counts_pdf_size_whole, base_pdf_size_whole = np.histogram(data_size_whole, bins=bins_size_whole, normed=False)

    print "start plotting!"

    cd = plt.semilogx(base_pdf_size_uniq[1:], counts_pdf_size_uniq, 'b-', linewidth=1, label='Repeat cnt. = 1')
    cd = plt.semilogx(base_pdf_size_shared[1:], counts_pdf_size_shared, 'r-', linewidth=1, label='Repeat cnt. > 1')
    cd = plt.semilogx(base_pdf_size_whole[1:], counts_pdf_size_whole, 'g-', linewidth=1, label='Whole')

    print "start labeling!"

    ax.set_ylim(0, 1)

    ax.set_xlabel(xlabel, fontsize=18)
    ax.set_ylabel(ylabel, fontsize=18)  # 24,>14
    ax.get_yaxis().set_tick_params(labelsize=18)
    ax.get_xaxis().set_tick_params(labelsize=18)

    plt.grid()
    plt.tight_layout()
    name = 'file_size_pdf.png'
    fig.savefig(name)
    eps = 'file_size_pdf.eps'
    fig.savefig(eps)


def draw_file_repeat_cnt():
    df = pandas.read_csv(draw_repeat_cnt)
    print("after loading file!")
    data = df.as_matrix()
    print(data)
    ylabel = 'Cumulative file probability'
    xlabel = 'File repeat count'

    fig = fig_size('min')
    
    ax = fig.add_subplot(111)
    
    bins = np.arange(np.ceil(data.min()), np.floor(data.max()))

    print "cdf and pdf calculating: bins = %d" % len(bins)
    counts_cdf, base_cdf = np.histogram(data, bins=bins, normed=True)

    cdf = np.cumsum(counts_cdf)

    print "start plotting!"

    cd = plt.semilogx(base_cdf[1:], cdf, 'b-', linewidth=1)

    print "start labeling!"
    ax.set_xlim(2, data.max())
    ax.set_ylim(0, 1)

    ax.set_xlabel(xlabel, fontsize=18)
    ax.set_ylabel(ylabel, fontsize=18)  # 24,>14
    ax.get_yaxis().set_tick_params(labelsize=18)
    ax.get_xaxis().set_tick_params(labelsize=18)

    plt.grid()
    plt.tight_layout()
    name = 'file_repeat_cnt_cdf.png'
    fig.savefig(name)
    eps = 'file_repeat_cnt_cdf.eps'
    fig.savefig(eps)
    
    """plot pdf"""

    ylabel = 'File probability'
    xlabel = 'File repeat count'

    bins = np.arange(np.ceil(data.min()), np.floor(data.max()))
    counts_pdf, base_pdf = np.histogram(data, bins=bins, normed=True)

    print "start plotting!"

    pd = plt.semilogx(base_pdf[1:], counts_pdf, 'b-', linewidth=1)

    #counts_pdf, base_pdf = np.histogram(data, bins=bins, normed=False)

    print "start plotting!"

    #pd = plt.hist(data, normed=False, bins=20)

    print "start labeling!"
    
    #ax.set_ylim(0, 1)
    ax.set_xlim(2, data.max())

    ax.set_xlabel(xlabel, fontsize=18)
    ax.set_ylabel(ylabel, fontsize=18)  # 24,>14
    ax.get_yaxis().set_tick_params(labelsize=18)
    ax.get_xaxis().set_tick_params(labelsize=18)

    plt.grid()
    plt.tight_layout()
    name = 'file_repeat_cnt_pdf.png'
    fig.savefig(name)
    eps = 'file_repeat_cnt_pdf.eps'
    fig.savefig(eps)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
