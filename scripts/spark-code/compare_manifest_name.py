
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
GRAPH_DIR = os.path.join(HDFS_DIR, 'graph')
#tmp_filename = 'sha256-d33e7d48b1b4d2902966200e28f31f642d329bf59c8410c8559dd2c059bf28fa-1501273930.87.json'
#tmp_filename = 'sha256-0000086583dd63d03a9e36820a3e8c8b85ac99d3ff19336c92ee793107e208eb-1500673662.87.json'
VAR_DIR = os.path.join(HDFS_DIR, 'var')

master = "spark://hulk0:7077"

conf =  SparkConf()\
	.setAppName('jsonsanalysis')\
	.setMaster(master)\
	.set("spark.executor.cores", 7)\
	.set("spark.driver.memory", "10g")\
	.set("spark.executor.memory", "40g")\
	.set("spark.driver.maxResultSize", "10g")\
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

filename = "/var/layer_db_json_files.parquet1"
input_absfilename = os.path.join(LOCAL_DIR, 'pull_cnt_with_filename.csv')#'manifest.lst')#'output_size_cnt_total_sum.parquet')#'cnt_bigger_1_uniq.parquet')
output = os.path.join(GRAPH_DIR, 'size_cnt_total_sum.csv')#"output_uniq_cnts_bigger_1.csv")

#df = spark.read.format('csv').load(input_absfilename)
#df = spark.read.load('/layer_db_jsons/1g_big_json.parquet')#
#df = spark.read.load('/local/output_uniq_files_cnts_bigger_1.parquet')#'/layer_db_jsons/1g_big_json.parquet')#'/var/layer_file_mapping_2tb_hdd.parquet')#'/local/output_unique_cnt_not_empty.parquet')#'/var/layer_file_mapping.parquet')#'/manifests/manifests.parquet1')#_with_filename_with_layer_id_not_null.parquetg_json.parquet')

LAYER_DB_JSON_DIR  = os.path.join(HDFS_DIR, "layer_db_jsons/*")
df = spark.read.parquet('/manifests/manifests_analyzed_or_not.parquet')#'/table/shared_ratio_layer.parquet')
#df.describe('shared_ratio').show()
manifest_name = df.filter(df.layer_id_analyzed_or_not == True).select('filename')#.show(20, False)
manifest_name.show(20, False)

conver_imagenames = F.udf(lambda s: s.split('-latest')[0]+'-latest')

manifest_name = manifest_name.select(conver_imagenames('filename').alias('filename'))#.show(20, False)

pull_cnt = spark.read.csv('/local/pull_cnt_with_filename.csv')
df = pull_cnt.select(pull_cnt._c2.alias('realname'), pull_cnt._c3.alias('filename'))
df.show(20, False)
realname = df.join(manifest_name, ['filename'], 'inner')
realname.show(20, False)
print("=============> %d", realname.count())

realname.select('realname').write.csv('/local/analyzed_non_library_imagename.csv')

#print(df.filter(df.layer_id_analyzed_or_not == True).count())
#df.printSchema()
#df.show()
#layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
#files = layer_db_df.selectExpr("explode(dirs) As structdirs").selectExpr(
 #       "explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
#regular_files = files.filter(files.sha256.isNotNull())
#df = regular_files.filter(regular_files.type.like("%empty%"))
#print("=====>not empty regular files = %d", df.count())
#print(regular_files.count())
#uniq_files_sha256 = spark.read.parquet(output_unique_filename)
#uniq_rows = regular_files.filter(regular_files.sha256.isin(uniq_files_sha256.sha256))
#empty = df.filter(df.sha256 == 'd41d8cd98f00b204e9800998ecf8427e')
#empty.show()
#empty.filter(empty.type != "empty").show()
#uniq_rows.groupby(uniq_rows.sha256).agg(F.collect_list(uniq_rows.file_info.stat_size)
#cnt = df.groupby(df.sha256).agg(F.collect_list(df.type))#agg(F.collect_list(regular_files.file_info.stat_size))
#cnt.show(20, False)
#cnt.write.save(output_unique_cnt_not_empty_filename)
#cnt = df.select(df.sha256).dropDuplicates()
#df.printSchema()
#df.show()
#print("==========> files: %d", cnt.count())
#new = df.select('dirs.files')
#new.show()
#new.printSchema()
#df.select('layer_id').show()
#df.select('sha256', 'total_sum', 'avg', 'cnt').write.csv(output)
#df.filter(~df.account_number.rlike("[ ,;{}()\n\t=]"))
#df.filter(df["`file_info.stat_size`"].rlike("[ ,;{}()\n\t=]")).show()
#df.select("count").write.csv(output)
#print(df.select('sha256').count())
#layer_id = df.select("layer_id")
#layer_id.show()
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
