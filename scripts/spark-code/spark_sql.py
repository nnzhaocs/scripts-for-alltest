
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

#spark.read.json(“/path/to/myDir”)
HDFS_DIR = 'hdfs://hulk0:8020/'
LAYER_JSONS_DIR1 = os.path.join(HDFS_DIR, 'nannan_2tb_hdd/layer_db_json')
LAYER_JSONS_DIR2 = os.path.join(HDFS_DIR, '2tb_hdd_3/layer_db_json')
TEMP_DIR = os.path.join(HDFS_DIR, 'temp')
LOCAL_DIR = os.path.join(HDFS_DIR, 'local')
#tmp_filename = 'sha256-d33e7d48b1b4d2902966200e28f31f642d329bf59c8410c8559dd2c059bf28fa-1501273930.87.json'
#tmp_filename = 'sha256-0000086583dd63d03a9e36820a3e8c8b85ac99d3ff19336c92ee793107e208eb-1500673662.87.json'

master = "spark://hulk0:7077"

conf =  SparkConf().setAppName('jsonsanalysis').setMaster(master)
sc = SparkContext(conf = conf)

#master = "spark://hulk0:7077"
spark = SparkSession.builder.appName("jsonsanalysis").config("spark.some.config.option", "some-value").master(master).getOrCreate()

spark.read.json(LAYER_JSONS_DIR1).select("layer_id").write.csv("/var/layer_id.csv")
#layer_id = df.select("layer_id")
#layer_id.createOrReplaceTempView("layer_id_tables")
#layer_id.cache()
#spark.table("layer_id_tables").write.saveAsTable('/var/layer_table')
#layer_id.write.csv("/var/layer_id.csv")

