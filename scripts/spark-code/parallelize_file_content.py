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

#lines = sc.textFile(os.path.join(LAYER_JSONS_DIR1, tmp_filename))

#lineLengths = lines.map(lambda s:len(s))
#totalLength = lineLengths.reduce(lambda a, b: a + b)

#df = sc.textFile(os.path.join(LAYER_JSONS_DIR1, tmp_filename))#.flatMap(lambda x:json.loads(x[1])).toDF()

#display(df)

#print("totalLength:", totalLength) 

#def filenamemapping(pair):
#	filename, content = pair

#master = "spark://hulk0:7077"
#spark = SparkSession.builder.appName("jsonsanalysis").config("spark.some.config.option", "some-value").master(master).getOrCreate()
"""
def load_dir(dir):
	dir_filelist = []
	for path, subdirs, files in os.walk(dir):
		for f in os.listdir(path):
                        if os.path.isfile(os.path.join(path, f)):
                            dir_filelist.append(os.path.join(path, f))
	return dir_filelist                       


def jsonToDF(json):
	print(json)
	#return json
	#reader = spark.read
	#if schema:
	#	reader.schema(schema)
	#return reader.json(sc.parallelize([json])) 
	df = spark.read.json(json)
	return df.select("layer_id")

#def selectMetrics(col):
"""		
#dir = TEMP_DIR
tmpfilename = os.path.join(LOCAL_DIR, 'tmp_files.txt')
jsonfilename = os.path.join(LOCAL_DIR, 'all_files.txt')

absfilenames = spark.read.text(tmpfilename).collect()
print(absfilenames)
dataframes = map(lambda r: spark.read.json(r[0]), absfilenames)
metricsData = reduce(lambda df1, df2: df1.unionAll(df2), dataframes)

#metricsData.show()

#dir_filelist = sc.wholeTextFiles(dir)#.collect()#.values()
#print(dir_filelist)
#metric_database = dir_filelist.flatMap(lambda x: spark.read.json(x[1]))

#metrics_collects = metric_database.collect()

#print(metrics_collects)
	
#json_filelist = glob.glob(LAYER_JSONS_DIR1)

#master = "spark://hulk0:7077"
#spark = SparkSession.builder.appName("jsonsanalysis").config("spark.some.config.option", "some-value").master(master).getOrCreate()
#df = spark.read.json(os.path.join('/temp',tmp_filename))#os.path.join(TEMP_DIR, tmp_filename))
#df = spark.read.json(TEMP_DIR)
#df = spark.read.json(sc.wholeTextFiles(LAYER_JSONS_DIR1).values())
#df.show()
""" 
df.printSchema()
df.select("layer_id").show() 
files = df.selectExpr("explode(dirs) As structdirs").selectExpr("explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
files.show()
#file_info = files.select("file_info")#Expr("explode(file_info) As structfile_info").selectExpr("structfile_info.*")
#file_info = 
files.select("file_info.link").show()
#files.select("file_info.link = null").show()
#file_size = 
files.filter(files.file_info.link.isNull()).show()
files.filter(files.file_info.link.link_type!="symlink").show()
#file_size.show()
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
"""

