# -*- coding: utf-8 -*-
import os, json
from pyspark import SparkContext, SparkConf
from IPython.display import clear_output, Image, display
from pyspark.sql import SparkSession
from pyspark.sql import Row
#from pyspark.sql import SQLContext as sqlContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import collections

#spark.read.json(“/path/to/myDir”)
HDFS_DIR = 'hdfs://hulk0:8020/'
LAYER_JSONS_DIR1 = os.path.join(HDFS_DIR, 'nannan_2tb_hdd/layer_db_json')
LAYER_JSONS_DIR2 = os.path.join(HDFS_DIR, '2tb_hdd_3/layer_db_json')
TEMP_DIR = os.path.join(HDFS_DIR, 'temp')
#tmp_filename = 'sha256-d33e7d48b1b4d2902966200e28f31f642d329bf59c8410c8559dd2c059bf28fa-1501273930.87.json'
tmp_filename = 'sha256-0000086583dd63d03a9e36820a3e8c8b85ac99d3ff19336c92ee793107e208eb-1500673662.87.json'
#conf =  SparkConf().setAppName('jsonsanalysis').setMaster("spark://hulk0:7077")
#sc = SparkContext(conf = conf)

#lines = sc.textFile(os.path.join(LAYER_JSONS_DIR1, tmp_filename))

#lineLengths = lines.map(lambda s:len(s))
#totalLength = lineLengths.reduce(lambda a, b: a + b)

#df = sc.textFile(os.path.join(LAYER_JSONS_DIR1, tmp_filename))#.flatMap(lambda x:json.loads(x[1])).toDF()

#display(df)

#print("totalLength:", totalLength) 

#def filenamemapping(pair):
#	filename, content = pair

spark = SparkSession.builder.appName("jsonsanalysis").config("spark.some.config.option", "some-value").master("local").getOrCreate()
df = spark.read.json(os.path.join('/temp',tmp_filename))#os.path.join(TEMP_DIR, tmp_filename))
#df.show()
df.printSchema()
df.select("layer_id").show() 
files = df.selectExpr("explode(dirs) As structdirs").selectExpr("explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
files.show()
#file_info = files.select("file_info")#Expr("explode(file_info) As structfile_info").selectExpr("structfile_info.*")
#file_info = 
files.select("file_info.link").show()
#display(file_info.select(struct(col("link"))))
#file_info.printSchema()
#sqlContext.registerDataFrameAsTable(file_info,"file_info").show()
#link = file_info.select("link")
#link.show()
#new_b = df.schema['file_info'].dataType.add(StructField('bb3', LongType()))
#print(file_info['link'])
#file_info.select(file_info['link']).show()
#dirs = df.select(df['dirs'])
#schemadirs = spark.createDataFrame(dirs)
#for i in dirs:
#	print(i)
#dirs.createOrReplaceTempView("dirs").collect()
#dirs.show()

#sqlDF = spark.sql("SELECT dir_size FROM dirs")
#sqlDF.show()


