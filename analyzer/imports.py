
"""imports all the libraries"""

import os, json, hashlib, argparse, logging, shutil, sys, time
from functools import reduce
from utility import *
import itertools
import threading, multiprocessing, Queue
# from  import Process,

import matplotlib
matplotlib.use('Agg')

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
import random, string, hashlib
#from optparse import OptionParser
import zipfile, tarfile
#from magic import Magic
import  magic
from collections import defaultdict
from itertools import groupby
import statistics
import subprocess
from collections import OrderedDict
from collections import defaultdict

os.system("taskset -p 0xff %d" % os.getpid())

q_dir_layers = []  # multiprocessing.Queue()

q_analyzed_layers = Queue.Queue()
# q_bad_unopen_layers = Queue.Queue()
#
# q_flush_analyzed_layers = Queue.Queue()
# q_flush_bad_unopen_layers = Queue.Queue()
#
# lock_f_bad_unopen_layer = Queue.Lock()
# lock_f_analyzed_layer = Queue.Lock()
#
# lock_q_analyzed_layer = Queue.Lock()

# =============================================

q_dir_images = multiprocessing.Queue()
q_layer_json_db = multiprocessing.Queue()

q_analyzed_images = multiprocessing.Queue()
q_bad_unopen_image_manifest = multiprocessing.Queue()
q_manifest_list_image = multiprocessing.Queue()

q_flush_analyzed_images = multiprocessing.Queue()

lock_f_bad_unopen_image_manifest = multiprocessing.Lock()
lock_f_analyzed_image_manifest = multiprocessing.Lock()
lock_f_manifest_list_image = multiprocessing.Lock()
lock_q_analyzed_image_manifest = multiprocessing.Lock()

lock_repo = multiprocessing.Lock()


# ===============================================

me = magic.Magic()  # me = magic.Magic(mime = True)