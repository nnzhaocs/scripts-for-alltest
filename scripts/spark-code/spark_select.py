
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

conf =  SparkConf()\
	.setAppName('jsonsanalysis')\
	.setMaster(master)\
	.set("spark.executor.cores", 7)\
	.set("spark.driver.memory", "10g")\
	.set("spark.executor.memory", "40g")\
	#.set("spark.sql.hive.filesourcePartitionFileCacheSize", "30g")
sc = SparkContext(conf = conf)

#master = "spark://hulk0:7077"
spark = SparkSession \
	.builder \
	.appName("jsonsanalysis")\
	.master(master)\
	.config(conf = conf)\
	.getOrCreate()
"""overwritten"""
#spark.read.json('/var/layer_db_json_1.json')#.write.json("/var/layer_id_1.csv")
df = spark.read.load("/var/layer_db_json_files.parquet1")
layer_id = df.select("layer_id")
layer_id.show()
#layer_id.createOrReplaceTempView("layer_id_tables")
#layer_id.cache()
#spark.table("layer_id_tables").write.saveAsTable('/var/layer_table')
#layer_id.write.csv("/var/layer_id.csv")
"""
files = df.selectExpr("explode(dirs) As structdirs").selectExpr("explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
files.show()
files.filter(files.file_info.link.isNull()).show()
files.filter(files.file_info.link.link_type!="symlink").show()

"""
