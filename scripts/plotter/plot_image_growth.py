
# import matplotlib, sys, os
# matplotlib.use('Agg')
#
# import numpy as np
# import matplotlib.pyplot as plt
# from matplotlib.ticker import MultipleLocator


# def get_data(filename)
#cmd1 = 'awk -F\'' + r'\t' + '\' \'{print $3}\' %s > tmp1.txt' % 'search_images_count-1.tsv'
#rc = os.system(cmd1)
#assert (rc == 0)
#
#cmd = 'awk -F\'' + r'\t' + '\' \'{print $3}\' %s > tmp-.txt' % 'search_images_count.tsv'
#rc = os.system(cmd)
#assert (rc == 0)
#
#data1 = np.loadtxt("tmp1.txt")

import sys
sys.path.append('../libraries/')
sys.path.append('../analyzer/')
from graph_related_libraries import *
from config import *

image_growth_filename = 'images-5-30-7-11-cnt.txt'

data0 = np.loadtxt(image_growth_filename)

images_cnt = [data0[i] for i in range(0, len(data0), 24)]

print images_cnt

print len(images_cnt)

data0 = np.trim_zeros(images_cnt)

print data0

print len(data0)

x = np.arange(0, len(data0), 1)

base = datetime.datetime(2017, 5, 30)
arr = np.array([base + datetime.timedelta(days=i) for i in xrange(len(data0))])

print arr

plt.rcParams.update({'font.size': 12})
fig = plt.figure(figsize=(12, 6), dpi=80)

ax = fig.add_subplot(111)

plt.xlabel('Time (Days)')
plt.ylabel('Number of images in Docker Hub')

ax.xaxis.set_major_locator(DayLocator(interval=3))
ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter('%m/%d'))

#ax.xaxis.set_major_locator(WeekdayLocator(byweekday=MO, interval=4))
#ax.set_xlim([datetime.date(2017, 5, 30), datetime.date(2017, 7, 11)])

#xmajorLocator = MultipleLocator(1)
#ax.xaxis.set_major_locator(xmajorLocator)
# plt.xlim(0, 200000)
line = plt.plot(arr, data0, 'b-')
plt.setp(line, color='r', linewidth=1.0)
#plt.xticks(np.arange(datetime.date(2017, 5, 30), datetime.date(2017, 7, 11), datetime.timedelta(days=3)))
# ax=fig.add_subplot(212)
#
# plt.xlabel('200K images (Name)')
# plt.ylabel('Pull count')
# #plt.title(pull count)
# xmajorLocator=MultipleLocator(10000)
# ax.xaxis.set_major_locator(xmajorLocator)
#plt.xlim(datetime.date(2017, 5, 30), datetime.date(2017, 7, 11), auto=True)
# plt.plot(x, data_pulls, 'r')
plt.savefig('image_growth.png')
