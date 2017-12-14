
# -*- coding: utf-8 -*-
import os
#, json, logging
from itertools import chain
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, collect_list, size
from pyspark.sql import functions as F


HDFS_DIR = 'hdfs://hulk0:8020/'
MANIFESTS_DIR2 = os.path.join(HDFS_DIR, "/2tb_hdd/manifests")
MANIFESTS_DIR1 = os.path.join(HDFS_DIR, "/var/tmpmanifestdir/")
VAR_DIR = os.path.join(HDFS_DIR, 'var')
LOCAL_DIR = os.path.join(HDFS_DIR, 'local')

MANIFESTS_DIR = os.path.join(HDFS_DIR, "manifests")
LAYER_DB_JSON_DIR  = os.path.join(HDFS_DIR, "layer_db_jsons/*")


pull_cnt_absfilename = os.path.join(LOCAL_DIR, "pull_cnt_with_filename.csv") #"_c3"
manifest_absfilename = os.path.join(MANIFESTS_DIR, "manifests_with_filename_with_layer_id_not_null.parquet")

layer_db_absfilename1 = os.path.join(LAYER_DB_JSON_DIR, "1g_big_json.parquet")
layer_db_absfilename2 = os.path.join(LAYER_DB_JSON_DIR, "2tb_hdd_json.parquet")
layer_db_absfilename3 = os.path.join(LAYER_DB_JSON_DIR, "nannan_2tb_json.parquet")


output_absfilename = os.path.join(VAR_DIR, 'manifests.parquet')
output_unique_filename = os.path.join(LOCAL_DIR, 'unique_file_ids.parquet')
output_unique_cnt_filename = os.path.join(LOCAL_DIR, 'unique_file_cnts.parquet') 
output_uniq_files_cnts_bigger_1 = os.path.join(LOCAL_DIR, 'output_uniq_files_cnts_bigger_1.parquet')


""".set("spark.executor.cores", 5) \
    .set("spark.driver.memory", "10g") \
    .set("spark.executor.memory", "40g") \ 
"""


EXECUTOR_CORES = 5
EXECUTOR_MEMORY = "40g"
DRIVER_MEMORY = "10g"

list_elem_num = 10000

master = "spark://hulk0:7077"

all_json_absfilename = os.path.join(LOCAL_DIR, 'manifest.lst')


def calculate_redundant_files_in_layers(spark, sc):
    cnt_bigger_1 = spark.read.parquet(output_uniq_files_cnts_bigger_1)
    sha256 = cnt_bigger_1.select("sha256")
    
    layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
    files = layer_db_df.selectExpr("explode(dirs) As structdirs").selectExpr(
        "explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
    regular_files = files.filter(files.sha256.isNotNull())

    cnt_bigger_1_df = regular_files.join(cnt_bigger_1, ['sha256'], 'leftsemi')
    cnt_bigger_1_df.show()
    #cnt_bigger_1_df.write.save()
    

def save_unique_files_cnt_bigger_1(spark, sc):
    cnt = spark.read.parquet(output_unique_cnt_filename)
    cnt_bigger_1 = cnt.where("count > 1")
    cnt_bigger_1.show()
    print(cnt_bigger_1.count())
    cnt_bigger_1.write.save(output_uniq_files_cnts_bigger_1)


def save_unqiue_files_cnts(spark, sc):
    layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
    files = layer_db_df.selectExpr("explode(dirs) As structdirs").selectExpr(
        "explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
    regular_files = files.filter(files.sha256.isNotNull())
    #print(regular_files.count())
    #uniq_files_sha256 = spark.read.parquet(output_unique_filename)
    #uniq_rows = regular_files.filter(regular_files.sha256.isin(uniq_files_sha256.sha256))
    
    #uniq_rows.groupby(uniq_rows.sha256).agg(F.collect_list(uniq_rows.file_info.stat_size)
    cnt = regular_files.groupby(regular_files.sha256).count().distinct()#agg(F.collect_list(regular_files.file_info.stat_size))
    cnt.write.save(output_unique_cnt_filename)


def save_unique_files(spark, sc):
    layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
    files = layer_db_df.selectExpr("explode(dirs) As structdirs").selectExpr(
        "explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
    regular_files = files.filter(files.sha256.isNotNull())
    uniq_files = regular_files.select(regular_files.sha256).dropDuplicates()
    """file_group = regular_files \
		.agg(collect_list(regular_files.sha256) \
		.alias("ids")) \
		.where(size("ids") > 1) """
    uniq_files.show()
    uniq_files.write.save(output_unique_filename)
    #sha256.show()
    #unique_sha = sha256.dropDuplicates()
    
    #print(sha256.count())


def init_spark_cluster():
    """
    conf = SparkConf().setAppName('jsonsanalysis').setMaster(master)
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName("jsonsanalysis").config("spark.some.config.option", "some-value").master(
        master).getOrCreate()
    """
    conf =  SparkConf()\
        .setAppName('jsonsanalysis')\
        .setMaster(master)\
        .set("spark.executor.cores", 5)\
        .set("spark.driver.memory", "10g")\
        .set("spark.executor.memory", "40g")\
        #.set("spark.sql.hive.filesourcePartitionFileCacheSize", "30g")
    sc = SparkContext(conf = conf)

    spark = SparkSession \
        .builder \
        .appName("jsonsanalysis")\
        .master(master)\
        .config(conf = conf)\
        .getOrCreate()

    return sc, spark


def main():

    sc, spark = init_spark_cluster()
    calculate_redundant_files_in_layers(spark, sc)
    # join_all_json_files(spark)
    #absfilename_list = spark.read.text(all_json_absfilename).collect()
    #absfilenames = [str(i.value) for i in absfilename_list]
    #print absfilename_list
    #join_json_files(absfilenames, spark)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
