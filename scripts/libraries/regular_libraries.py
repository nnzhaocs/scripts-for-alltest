
"""imports all the regular libraries"""

import os, sys, time, logging, traceback
reload(sys)  
sys.setdefaultencoding('utf8')
import argparse, optparse

import json

from stat import *

import hashlib

import threading, multiprocessing, Queue

import zipfile, tarfile

# from config import *

import magic

"""other libraries"""
from functools import partial
# json, hashlib, argparse, logging, shutil, sys, time, optparse
# import itertools
# import random, string
from collections import defaultdict
# from itertools import groupby
from statistics  import mean
import statistics
import subprocess
# from collections import OrderedDict
# from collections import defaultdict
