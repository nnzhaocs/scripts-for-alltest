#import matplotlib, sys, os
#matplotlib.use('Agg')
#
#import numpy as np
#import matplotlib.pyplot as plt
#from matplotlib.ticker import MultipleLocator
#import argparse
#from optparse import OptionParser
#
##cmd_stars='awk -F\''+r'\t'+'\' \'{print $1}\' %s > image_stars.txt' % name
##cmd_pulls='awk -F\''+r'\t'+'\' \'{print $2}\' %s > image_pulls.txt' % name
##
##os.system(cmd_stars)
##os.system(cmd_pulls)
##
##columns_stars=np.loadtxt('image_stars.txt')
##columns_pulls=np.loadtxt('image_pulls.txt')
##
##print columns_stars
##print columns_pulls
##
###linestyles=['_', '-', '--', ]
###with open ('image_size_list.txt') as f:
##	#data=f.read()
##
###data=data.split('\n')
###x=[row.split('\t')[0] for row in data]
###x=filter(None, x)
###x=np.array(x, dtype='|S4').astype(np.float)
##
##data_stars=columns_stars
##data_pulls=columns_pulls
#
##print data_stars.min()
##print data_stars.max()
##print np.median(data_stars)
##fout=open('output.txt', 'w+')
#
#def plot_count(data, attr, ticks):
#	print data.min()
#	print data.max()
#	print np.median(data)	
#	
#	#bins=np.arange(np.floor(data.min()), np.ceil(data.max()))
#	bins=np.arange(np.floor(data.min()), np.ceil(data.max()) + ticks/20, ticks/20)
#	counts, base=np.histogram(data, bins=bins, density=False)
#
#	fig=plt.figure(figsize=(128,16), dpi=80)
#	ax=fig.add_subplot(111)
#
#	plt.plot(base[1:], counts, marker='o')
#	
#	plt.rcParams.update({'font.size': 22})
#	plt.xlim(ticks/20, ticks)
#	xmajorLocator=MultipleLocator(ticks/20)
#	ax.xaxis.set_major_locator(xmajorLocator)
#	#ax.set_xticks(np.arange(0, 5001, 128))
#	plt.xlabel(attr)
#	plt.ylabel('Count')
#	sstr0='Histogram of 1688 images\' %s: MIN:%d; MAX:%d; MEDIAN:%d' % (attr, data.min(), data.max(), np.median(data))
#	plt.title(sstr0)
#	plt.grid()
#	name = 'count_%s.png' % attr
#	plt.savefig(name)
#	
#def plot_cdf(data, attr, ticks):
#        print data.min()
#        print data.max()
#        print np.median(data)
#
#	bins=np.arange(np.floor(data.min()), np.ceil(data.max()))  
#	counts, base=np.histogram(data, bins=bins, density=1)
#	
#	cdf=np.cumsum(counts)
#	
#	plt.rcParams.update({'font.size': 22})
#	fig=plt.figure(figsize=(128,16), dpi=80)
#	ax=fig.add_subplot(111)
#	
#	ax.plot(base[1:], cdf, marker='o')
#	
#	plt.xlim(0, ticks)
#	xmajorLocator=MultipleLocator(ticks/20)
#	ax.xaxis.set_major_locator(xmajorLocator)
#	#ax.set_xticks(np.arange(0, 5001, 128))
#	plt.xlabel(attr)
#	plt.ylabel('CDF')
#	sstr0 = 'Cumulative distribution for 1688 images\' %s: MIN:%d; MAX:%d; MEDIAN:%d' % (attr, data.min(), data.max(), np.median(data))
#	plt.title(sstr0)
#	plt.grid()
#	name = 'cdf_%s.png' % attr
#	plt.savefig(name)

# import cStringIO
from imports import *
from draw_pic import *
from utility import *
# from file import *

def main():
        parser = optparse.OptionParser()
        parser.add_option('-f', '--filename', action='store', dest="filename", help="The input file name. e.g., images.tsv", default="images.tsv")
        options, args = parser.parse_args()
        #print parser.filename
        print 'Input file name: ', options.filename

	cmd_stars='awk -F\''+r','+'\' \'{print $1}\' %s > image_stars.txt' % options.filename
	cmd_pulls='awk -F\''+r','+'\' \'{print $2}\' %s > image_pulls.txt' % options.filename
	print cmd_stars
	print cmd_pulls

	os.system(cmd_stars)
	os.system(cmd_pulls)
	
	columns_stars=np.loadtxt('image_stars.txt')
	columns_pulls=np.loadtxt('image_pulls.txt')
	
	print columns_stars
	print columns_pulls
	
	data_stars=columns_stars
	data_pulls=columns_pulls

	#plot_count(data_stars, 'stars', 50)
	#plot_count(data_pulls, 'pulls', 2000)
	#plot_cdf(data_stars, 'stars', 128)
	#plot_cdf(data_pulls, 'pulls', 5000)

	fig = fig_size('small')  # 'large'

	data = data_stars
	xlabel = 'Star count for each image'  # data = [x * 1.0 / 1024 / 1024 for x in data1]
	xlim = 25  # max(data1)
	ticks = 25
	print xlim
	plot_cdf(fig, data, xlabel, xlim, ticks)
	
	fig = fig_size('small')  # 'large'
	
	data = data_pulls
	xlabel = 'Pull count for each image'  # data = [x * 1.0 / 1024 / 1024 for x in data1]
	xlim = 2000  # max(data1)
	ticks = 25
	print xlim
	plot_cdf(fig, data, xlabel, xlim, ticks)

if __name__=='__main__':
	print 'here'
	main()
	print 'finished!'
	exit(0)

