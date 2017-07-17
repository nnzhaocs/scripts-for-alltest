
from imports import *
from pylab import *
import itertools


def fig_size(size):
    if size == 'small':
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
            ax.text(a, b + 0.05, '%d' % int(b))


def plot_cdf(fig, data1, xlabel, xlim, ticks):
    data = np.array(data1)
    print(data.min())
    print(data.max())
    print(np.median(data))

    ax = fig.add_subplot(111)
    ax.set_xlim(0, xlim)
    xmajorLocator = MultipleLocator(xlim/ticks)
    ax.xaxis.set_major_locator(xmajorLocator)

    #bins_max = np.arange(np.ceil(data.min()), np.floor(data.max()))
    #bins = (data.max() - data.min())/(xlim/ticks)
    # bins = np.arange(np.ceil(data.min()), np.floor(data.max()) + xlim/ticks, xlim/ticks)
    bins = ticks
    print "cdf and pdf calculating: bins = %d" % len(bins)
    counts_cdf, base_cdf = np.histogram(data, bins=bins, normed=True)
    counts_pdf, base_pdf = np.histogram(data, bins=bins, normed=True)
    cdf = np.cumsum(counts_cdf)

    print "start plotting!"
    pd = ax.bar(base_cdf[1:] - 0.4, counts_cdf, width=0.4, color='r', label='Probability distribution', align='center')
    cd = ax.bar(base_pdf[1:] + 0.0, cdf, color='b', width=0.4, label='Cumulative distribution', align='center')

    print "start labeling!"
    bar_label_text(ax, base_cdf[1:] - 0.4, counts_cdf, xlim)
    bar_label_text(ax, base_pdf[1:] + 0.0, cdf, xlim)

    ax.set_xlabel(xlabel, fontsize=22)
    ax.set_ylabel('Distribution', fontsize=22)

    plt.legend([pd, cd], ['Probability distribution', 'Cumulative distribution'])

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
    ax.set_xlabel(xlabel, fontsize=22)
    ax.set_ylabel(ylabel, fontsize=22)

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

