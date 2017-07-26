
import matplotlib, sys, os
matplotlib.use('Agg')

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator


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
data0 = np.loadtxt("tmp.txt")

#data0 = np.concatenate((data1, data), axis=0)

print data0

data0 = np.trim_zeros(data0)

print len(data0)

x = np.arange(0, len(data0), 1)


plt.rcParams.update({'font.size': 22})
fig = plt.figure(figsize=(24, 8), dpi=80)

ax = fig.add_subplot(111)

plt.xlabel('Time (Days)')
plt.ylabel('Number of images in docker Hub')

xmajorLocator = MultipleLocator(1)
ax.xaxis.set_major_locator(xmajorLocator)
# plt.xlim(0, 200000)
line = plt.plot([x1/24 for x1 in x ], data0, 'b-')
plt.setp(line, color='r', linewidth=2.0)



# ax=fig.add_subplot(212)
#
# plt.xlabel('200K images (Name)')
# plt.ylabel('Pull count')
# #plt.title(pull count)
# xmajorLocator=MultipleLocator(10000)
# ax.xaxis.set_major_locator(xmajorLocator)
# plt.xlim(0, 200000)
# plt.plot(x, data_pulls, 'r')
plt.savefig('image_growth.png')
