#!/bin/env python
import os, json, hashlib, argparse, logging, shutil, sys
from functools import reduce
from utility import *
import itertools
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



