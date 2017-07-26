
from imports import *
from pylab import *
import itertools


def fig_size(size):
    if size == 'small':
	fig = plt.figure(figsize=(24, 8), dpi=80)
    if size == 'median':
        fig = plt.figure(figsize=(48, 16), dpi=80)
    elif size == 'large':
        fig = plt.figure(figsize=(128, 16), dpi=80)
    return fig


def bar_label_text(ax, x, y, xlim):
    for a, b in zip(x, y):
        if a > xlim:
            break
        if isinstance(b, int):
            ax.text(a, b + 0.05, '%d' % b)
        else:
            ax.text(a, b + 0.05, '%.1f' % b)


def plot_cdf(fig, data1, xlabel, xlim, ticks):
    data = np.array(data1)
    print("plot: min = %d" % data.min())
    print("plot: max = %d" % data.max())
    print("plot: median = %d" % np.median(data))

    ax = fig.add_subplot(111)
    ax.set_xlim(0, xlim)
    xmajorLocator = MultipleLocator(xlim/ticks)
    ax.xaxis.set_major_locator(xmajorLocator)

    bins = np.arange(np.ceil(data.min()), np.floor(data.max()))
    #bins = (data.max() - data.min())/(xlim/ticks)

#     # bins = np.arange(np.ceil(data.min()), np.floor(data.max()) + xlim/ticks, xlim/ticks)
#     bins = ticks
#     print "cdf and pdf calculating: bins = %d" % len(bins)
#     counts_cdf, base_cdf = np.histogram(data, bins=bins, normed=True)
#     counts_pdf, base_pdf = np.histogram(data, bins=bins, normed=True)
#     cdf = np.cumsum(counts_cdf)
    #bins = np.arange(data.min(), data.max() + xlim/ticks, xlim/ticks)
    # bins = np.arange(np.ceil(data.min()), np.floor(data.max()) + xlim/ticks, xlim/ticks)
    #bins = np.arange(np.floor(data.min()), np.ceil(data.max()) + xlim/ticks - np.ceil(data.max()) % (xlim/ticks), xlim/ticks)
    print "cdf and pdf calculating: bins = %d" % len(bins)
    counts_cdf, base_cdf = np.histogram(data, bins=bins, normed=True)
    counts_pdf, base_pdf = np.histogram(data, bins=bins, normed=False)
    cdf = np.cumsum(counts_cdf)
    #pdf = np.cumsum(counts_pdf)
    #cdf_ =  [x * 1.0 / len(data) for x in pdf]
    #counts_pdf_ = [x * 1.0 / len(data) for x in counts_pdf]
    # print (data, len(data), counts_cdf, cdf, base_cdf)
    #print (cdf_, counts_pdf_)
    print "start plotting!"
    pd = ax.plot(base_cdf[1:], counts_cdf, 'r-', linewidth=2, label='Normalized Probability distribution')
    cd = ax.plot(base_cdf[1:], cdf, 'b-', linewidth=2, label='Normalized Cumulative distribution')
    ax2 = ax.twinx()
    nd = ax2.plot(base_cdf[1:], counts_pdf, 'g--', linewidth=2, label='Probability distribution')
    print "start labeling!"
    # bar_label_text(ax, base_pdf[1:] - 0.4, counts_pdf_, xlim)
    # bar_label_text(ax, base_pdf[1:] + 0.0, cdf_, xlim)
    ax.set_ylim(0, 1)
    ax2.set_ylim(0, max(counts_pdf))
    ax.set_xlabel(xlabel, fontsize=22)
    ax.set_ylabel('Normalized Probability', fontsize=22)
    ax2.set_ylabel('Probability', fontsize=22)

    plt.legend(pd+cd+nd, [l.get_label() for l in (pd+cd+nd)], loc=0)

    #ax2 = ax1.twinx()
    #ax2 = fig.add_subplot(111)#bins = ticks,
    #bins = np.arange(np.floor(data.min()), np.ceil(data.max()))
   
    sstr0 = 'Distribution of %s: MIN:%d; MAX:%d; MEDIAN:%d' % (xlabel, data.min(), data.max(), np.median(data))
    plt.title(sstr0)
    plt.grid()
    name = '2distribution%s.png' % xlabel
    fig.savefig(name)


def plot_bar_pic(fig, x, y, xlabel, ylabel, xlim, ticks):
    #fig, ax = plt.subplots()
    ax = fig.add_subplot(111)

    ax.set_xlim(xlim / ticks, xlim)
    ax.set_xlabel(xlabel, fontsize=24)
    ax.set_ylabel(ylabel, fontsize=24)

    # plt.xlabel(xlabel)
    # plt.ylabel(ylabel)
    #plt.title('Average layer size for layers with same popularity (layers with same repeat count)')
    xmajorLocator = MultipleLocator(xlim / ticks) 
    ax.xaxis.set_major_locator(xmajorLocator)
    #fig = plt.figure()
    width = 0.5 
    #for i in y: 
    #   ax.text(rect.get_x(), i)
    #ind = np.array()
    #plt.xlim(0, xlim)
    ax.bar(x, y, width=width)
    ax.grid()
    name = 'bar_%s%s' % (xlabel, ylabel)

    bar_label_text(ax, x, y, xlim)
    #rect = ax.bar(x, y, width = width)
    #autolabel(rect)
    #for i in y:
        #ax.text(rect.get_x(), i, i) 
    fig.savefig(name)

