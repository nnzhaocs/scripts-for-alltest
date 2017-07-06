
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

from magic import Magic
from collections import defaultdict
from itertools import groupby

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


