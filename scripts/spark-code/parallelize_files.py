# -*- coding: utf-8 -*-
import os, json
from pyspark import SparkContext, SparkConf
from IPython.display import clear_output, Image, display
from pyspark.sql import SparkSession
from pyspark.sql import Row
#from pyspark.sql import SQLContext as sqlContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
import collections

HDFS_DIR = 'hdfs://hulk0:8020/'
LAYER_JSONS_DIR1 = os.path.join(HDFS_DIR, 'nannan_2tb_hdd/layer_db_json')
LAYER_JSONS_DIR2 = os.path.join(HDFS_DIR, '2tb_hdd_3/layer_db_json')
TEMP_DIR = os.path.join(HDFS_DIR, 'temp')
LOCAL_DIR = os.path.join(HDFS_DIR, 'local')

master = "spark://hulk0:7077"

conf =  SparkConf().setAppName('jsonsanalysis').setMaster(master)
sc = SparkContext(conf = conf)

spark = SparkSession.builder.appName("jsonsanalysis").config("spark.some.config.option", "some-value").master(master).getOrCreate()

tmpfilename = os.path.join(LOCAL_DIR, 'tmp_files.txt')
jsonfilename = os.path.join(LOCAL_DIR, 'all_files.txt')

def parse_layer_json(json_data, type='stat_size'):
    """file stat_type file type file size"""
    base_types = ['type', 'sha256']
    stat_types = ['stat_type', 'stat_size']

    file_metrics_data = []
    #layer_json_absfilename = os.path.join(HDFS_DIR, _layer_json_absfilename)
    #with open(layer_json_absfilename) as lj_f:
        #json_data = json_load(lj_f)
    #return json_data['dirs']
    if not json_data['dirs']:
	return file_metrics_data
    for subdir in json_data['dirs']:
	if not subdir['files']:
	    continue
        for sub_file in subdir['files']:
            if type in base_types:
                file_metrics_data.append(sub_file[type])
            elif type in stat_types:
                file_metrics_data.append(sub_file['file_info'][type])
    return file_metrics_data


#absfilenames
rawData = sc.wholeTextFiles(LAYER_JSONS_DIR1).map(lambda x: json.loads(x[1])).flatMap(parse_layer_json)
#spark.read.text(tmpfilename).collect()
#print(absfilenames)

#rawData = absfilenames.rdd.map(lambda x: sc.textFile(x[0]))#.flatMap(parse_layer_json)
metricsData = rawData.saveAsTextFile('/var/stat_size.txt')

#metricsData.show()


