# -*- coding: utf-8 -*-
import os, json
from pyspark import SparkContext, SparkConf
from IPython.display import clear_output, Image, display

#spark.read.json(“/path/to/myDir”)
HDFS_DIR = 'hdfs://hulk0:8020/'
LAYER_JSONS_DIR1 = os.path.join(HDFS_DIR, 'nannan_2tb_hdd/layer_db_json')
LAYER_JSONS_DIR2 = os.path.join(HDFS_DIR, '2tb_hdd_3/layer_db_json')

tmp_filename = 'sha256-d33e7d48b1b4d2902966200e28f31f642d329bf59c8410c8559dd2c059bf28fa-1501273930.87.json'

conf =  SparkConf().setAppName('jsonsanalysis').setMaster("spark://hulk0:7077")
sc = SparkContext(conf = conf)

lines = sc.textFile(os.path.join(LAYER_JSONS_DIR1, tmp_filename))

lineLengths = lines.map(lambda s:len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)

df = sc.textFile(os.path.join(LAYER_JSONS_DIR1, tmp_filename))#.flatMap(lambda x:json.loads(x[1])).toDF()

display(df)

print("totalLength:", totalLength) 

