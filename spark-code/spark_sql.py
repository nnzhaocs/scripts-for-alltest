
# -*- coding: utf-8 -*-
import os, json, ijson
from pyspark import SparkContext, SparkConf
from IPython.display import clear_output, Image, display
from pyspark.sql import SparkSession
from pyspark.sql import Row, SQLContext
#from pyspark.sql import SQLContext as sqlContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
import collections
#import SQLContext

#spark.read.json(“/path/to/myDir”)
HDFS_DIR = 'hdfs://hulk0:8020/'
LAYER_JSONS_DIR1 = os.path.join(HDFS_DIR, 'nannan_2tb_hdd/layer_db_json')
LAYER_JSONS_DIR2 = os.path.join(HDFS_DIR, '2tb_hdd_3/layer_db_json')
TEMP_DIR = os.path.join(HDFS_DIR, 'temp')
LOCAL_DIR = os.path.join(HDFS_DIR, 'local')
#tmp_filename = 'sha256-d33e7d48b1b4d2902966200e28f31f642d329bf59c8410c8559dd2c059bf28fa-1501273930.87.json'
#tmp_filename = 'sha256-0000086583dd63d03a9e36820a3e8c8b85ac99d3ff19336c92ee793107e208eb-1500673662.87.json'

temp_filename1 = '/2tb_hdd_3/layer_db_json/sha256-06cc1ee2359e4ceb90f832c8c86c06dd82705ed7eefe0a62948c4cb6225c7969-1505184549.14.json'

temp_filename = '/var/1gb_json.json'


master = "spark://hulk0:7077"

#conf =  SparkConf().setAppName('jsonsanalysis').setMaster(master)
#sc = SparkContext(conf = conf)

#master = "spark://hulk0:7077"
#spark = SparkSession.builder.appName("jsonsanalysis").config("spark.some.config.option", "some-value").master(master).getOrCreate()

conf =  SparkConf()\
        .setAppName('jsonsanalysis')\
        .setMaster(master)\
        .set("spark.executor.cores", 5)\
        .set("spark.driver.memory", "10g")\
        .set("spark.executor.memory", "40g")\
	.set("spark.driver.maxResultSize", "2g")
        #.set("spark.sql.hive.filesourcePartitionFileCacheSize", "30g")
sc = SparkContext(conf = conf)

spark = SparkSession \
        .builder \
        .appName("jsonsanalysis")\
        .master(master)\
        .config(conf = conf)\
        .getOrCreate()

sqlContext = SQLContext(spark.sparkContext)


spark.read.json(temp_filename1, multiLine=True).coalesce(40).write.save("/var/1g_big_json.parquet", format="parquet", mode='append')

#spark.read.json(temp_filename, multiLine=True).write.save("/var/1g_json.parquet", format="parquet", mode='append')

#df 
#spark.read.json(sc.wholeTextFiles(temp_filename)).write.save("/var/1g_json.parquet", format="parquet")
#sqlContext.jsonFile("data.json")	
#string = sc.wholeTextFiles(temp_filename).flatMap(lambda x: ijson.parser(x[1]))
#print(stringlist)
#string = [str(i.value) for i in stringlist]
#df = sqlContext.createDataFrame(string)
#df.write.save("/var/1g_json.parquet", format="parquet")
#rdd = sc.textFile(temp_filename)
#rdd.map(json.loads).cache().toDF().write.save("/var/1g_json.parquet", format="parquet")
#saveAsParquetFile("/var/1g_json.parquet")
#layer_id = df.select("layer_id")
#layer_id.createOrReplaceTempView("layer_id_tables")
#layer_id.cache()
#spark.table("layer_id_tables").write.saveAsTable('/var/layer_table')
#layer_id.write.csv("/var/layer_id.csv")

