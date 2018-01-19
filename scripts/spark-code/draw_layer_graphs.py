import sys

sys.path.append('../plotter/')
from draw_pic import *
# from analysis_library import *
import pandas as pd

RESULTS_DIR = '/home/nannan/4tb_results'



draw_layer_shared_cnt = os.path.join(RESULTS_DIR, 'draw_layer_shared_cnt_data.csv')

# draw_type_by_repeat_cnt = os.path.join(RESULTS_DIR, 'draw_type_by_repeat_cnt.csv')
# draw_type_by_total_sum = os.path.join(RESULTS_DIR, 'draw_type_by_total_sum.csv')
# draw_type1_by_repeat_cnt = os.path.join(RESULTS_DIR, 'draw_type1_by_repeat_cnt.csv')
# draw_type_by_size = os.path.join(RESULTS_DIR, 'draw_type_by_size.csv')
# draw_type_by_dup_ratio_cap = os.path.join(RESULTS_DIR, 'draw_type_by_dup_ratio_cap.csv')
# draw_type_by_dup_ratio_cnt = os.path.join(RESULTS_DIR, 'draw_type_by_dup_ratio_cnt.csv')
#
# capacity_data = os.path.join(RESULTS_DIR, 'capacity_data.csv')
#
# draw_size_uniq = os.path.join(RESULTS_DIR, 'draw_size_uniq_data.csv')
# draw_size_shared = os.path.join(RESULTS_DIR, 'draw_size_shared_data.csv')
# draw_size_whole = os.path.join(RESULTS_DIR, 'draw_size_whole_data.csv')
# draw_repeat_cnt = os.path.join(RESULTS_DIR, 'draw_repeat_cnt_data.csv')


def main():
    draw_layer_shared_cnt()


def draw_layer_shared_cnt():
    df = pd.read_csv(draw_layer_shared_cnt)
    print("after loading file!")
    data = df.as_matrix()
    print(data)
    ylabel = 'Cumulative layer probability'
    xlabel = 'Layer reference count'

    fig = fig_size('min')

    ax = fig.add_subplot(111)

    bins = np.arange(np.ceil(data.min()), np.floor(data.max()))

    print "cdf and pdf calculating: bins = %d" % len(bins)
    """
    # =================> plot cdf
    counts_cdf, base_cdf = np.histogram(data, bins=bins, normed=True)

    cdf = np.cumsum(counts_cdf)

    print "start plotting!"

    cd = plt.semilogx(base_cdf[1:], cdf, 'b-', linewidth=1)

    print "start labeling!"

    ax.set_ylim(0, 1)

    ax.set_xlabel(xlabel, fontsize=18)
    ax.set_ylabel(ylabel, fontsize=18)  # 24,>14
    ax.get_yaxis().set_tick_params(labelsize=18)
    ax.get_xaxis().set_tick_params(labelsize=18)

    plt.grid()
    plt.tight_layout()
    name = 'layer_repeat_cnt_cdf.png'
    fig.savefig(name)
    eps = 'layer_repeat_cnt_cdf.eps'
    fig.savefig(eps)
    """

    # ===================> plot pdf

    ylabel = 'Layer frequency'
    xlabel = 'Reference cnt'

    counts_pdf, base_pdf = np.histogram(data, bins=bins, normed=False)

    print "start plotting!"

    pd = plt.semilogx(base_pdf[1:], counts_pdf, 'b-', linewidth=1)

    print "start labeling!"

    ax.set_ylim(0, 1)

    ax.set_xlabel(xlabel, fontsize=18)
    ax.set_ylabel(ylabel, fontsize=18)  # 24,>14
    ax.get_yaxis().set_tick_params(labelsize=18)
    ax.get_xaxis().set_tick_params(labelsize=18)

    plt.grid()
    plt.tight_layout()
    name = 'layer_repeat_cnt_pdf.png'
    fig.savefig(name)
    eps = 'layer_repeat_cnt_pdf.eps'
    fig.savefig(eps)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'