
"""imports all the libraries"""

import os, json, hashlib, argparse, logging, shutil, sys, time
from functools import reduce
from utility import *
import itertools
import threading, Queue

import matplotlib
matplotlib.use('Agg')

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
import random, string, hashlib
#from optparse import OptionParser
import zipfile, tarfile
from magic import Magic
from collections import defaultdict
from itertools import groupby
import statistics
import subprocess

q_dir_layers = Queue.Queue()
# q_downloaded_layers = Queue.Queue()
q_analyzed_layers = Queue.Queue()
q_bad_unopen_layers = Queue.Queue()

q_flush_analyzed_layers = Queue.Queue()
q_flush_bad_unopen_layers = Queue.Queue()

# layer_q = Queue.Queue()
lock_f_bad_unopen_layer = threading.Lock()
lock_f_analyzed_layer = threading.Lock()
lock_q_analyzed_layer = threading.Lock()

# =============================================

q_dir_images = Queue.Queue()
q_layer_json_db = Queue.Queue()

q_analyzed_images = Queue.Queue()
q_bad_unopen_image_manifest = Queue.Queue()
q_manifest_list_image = Queue.Queue()

q_flush_analyzed_images = Queue.Queue()
# q_flush_bad_unopen_image_manifest = Queue.Queue()
# q_flush_bad_unopen_image_manifest = Queue.Queue()
# layer_q = Queue.Queue()
lock_f_bad_unopen_image_manifest = threading.Lock()
lock_f_analyzed_image_manifest = threading.Lock()
lock_f_manifest_list_image = threading.Lock()
lock_q_analyzed_image_manifest = threading.Lock()
# lock_q_analyzed_image_manifest = threading.Lock()

lock_repo = threading.Lock()

# f_manifest_list_image

# ===============================================

