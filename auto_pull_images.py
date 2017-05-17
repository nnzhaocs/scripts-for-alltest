
import os, sys, subprocess, select, random, urllib2, time, json, tempfile, shutil
import re
import threading, Queue

q=Queue.Queue()
num_worker_threads=50

def pull(image):
	cmd='docker pull %s' % image
        rc = os.system(cmd)

def operation():
	while True:
		name=q.get()
        	pull(name)
		q.task_done()
       
def get_image_names():
	cmd1='cp images.xls image-list.xls'
	cmd2='awk -F\''+r'\t'+'\' \'{print $7}\' image-list.xls > image-names.xls'
	print cmd1
	print cmd2
	rc=os.system(cmd1)
	assert(rc == 0)
	rc=os.system(cmd2)
	assert(rc == 0)

def queue_names():
	with open('image-names.xls') as fd:
		for name in fd: 
	        	print name
			q.put(name)

threads=[]
def main():
	get_image_names()
	queue_names()

	for i in range(num_worker_threads): 
		t=threading.Thread(target=operation)
		t.start()
		threads.append(t)

	q.join()
	for i in range(num_worker_threads):
		q.put(None)
	for t in threads:
		t.join()

if __name__=='__main__':
	main()
	exit(0)


