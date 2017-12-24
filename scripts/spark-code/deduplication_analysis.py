
# -*- coding: utf-8 -*-
import os
#, json, logging
from itertools import chain
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Window
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
LAYER_FILE_MAPPING_DIR = os.path.join(HDFS_DIR, "layer_file_mapping")


pull_cnt_absfilename = os.path.join(LOCAL_DIR, "pull_cnt_with_filename.csv") #"_c3"
manifest_absfilename = os.path.join(MANIFESTS_DIR, "manifests_with_filename_with_layer_id_not_null.parquet")

layer_db_absfilename1 = os.path.join(LAYER_DB_JSON_DIR, "1g_big_json.parquet")
layer_db_absfilename2 = os.path.join(LAYER_DB_JSON_DIR, "2tb_hdd_json.parquet")
layer_db_absfilename3 = os.path.join(LAYER_DB_JSON_DIR, "nannan_2tb_json.parquet")

#output_absfilename = os.path.join(VAR_DIR, 'manifests.parquet')
output_unique_filename = os.path.join(LOCAL_DIR, 'unique_file_ids.parquet')
output_unique_cnt_filename = os.path.join(LOCAL_DIR, 'unique_file_cnts.parquet') 
output_uniq_files_cnts_bigger_1 = os.path.join(LOCAL_DIR, 'output_uniq_files_cnts_bigger_1.parquet')
output_uniq_cnts_sizes = os.path.join(LOCAL_DIR, 'output_uniq_cnts_sizes.parquet')
cnt_bigger_1_uniq = os.path.join(LOCAL_DIR, 'cnt_bigger_1_uniq.parquet') 
cnt_bigger_1_fileinfo = os.path.join(LOCAL_DIR, 'cnt_bigger_1_fileinfo.parquet')
output_size_cnt_total_sum = os.path.join(LOCAL_DIR, 'output_size_cnt_total_sum.parquet')

# output_unique_cnt_not_empty_filename = os.path.join(LOCAL_DIR, 'output_unique_cnt_not_empty.parquet')
# output_uniq_files_cnts_bigger_1_not_empty = os.path.join(LOCAL_DIR, 'output_uniq_files_cnts_bigger_1_not_empty.parquet')

""".set("spark.executor.cores", 5) \
    .set("spark.driver.memory", "10g") \
    .set("spark.executor.memory", "40g") \ 
"""

EXECUTOR_CORES = 5
EXECUTOR_MEMORY = "40g"
DRIVER_MEMORY = "10g"

# list_elem_num = 10000

master = "spark://hulk0:7077"

# all_json_absfilename = os.path.join(LOCAL_DIR, 'manifest.lst')

def main():

    sc, spark = init_spark_cluster()
    #save_unqiue_files_cnts(spark, sc)
    calculate_redundant_files_in_layers(spark, sc)
    #save_cnt_bigger_1_files(spark, sc)
    #save_sha256_sizes(spark, sc)
    #cumulative_sum_cal(spark, sc)
    #find_file(spark, sc)
    #find_file_in_layer(spark, sc)
    #save_cnt_bigger_1_fileinfo(spark, sc)
    #calculate_redundant_files_in_layers(spark, sc)
    #join_all_json_files(spark)
    #absfilename_list = spark.read.text(all_json_absfilename).collect()
    #absfilenames = [str(i.value) for i in absfilename_list]
    #print absfilename_list
    #join_json_files(absfilenames, spark)

# def get_layer_shared_file_proportion(spark, sc):
#     output_unique_filename


def calculate_redundant_files_in_layers(spark, sc):
    df_f = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    #df = df_f.select('sha256', F.sum(collec'file_size').alias('total_sum'), F.size('file_size').alias('cnt'), F.avg('file_size').alias('avg'))
    df_f.printSchema()
    df_f.show()
    # df_f.write.save(output_size_cnt_total_sum)


def find_file(spark, sc):
    df_f = spark.read.parquet(cnt_bigger_1_fileinfo)
    df_f.printSchema()
    df_f.show()
    df_f.filter(df_f.sha256 == 'ad52c20d539762f3c44bf69b5162d694').show()
    print(df_f.filter(df_f.sha256 == 'ad52c20d539762f3c44bf69b5162d694').take(2))#'d41d8cd98f00b204e9800998ecf8427e').show()


def find_file_in_layer(spark, sc):
    layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
    #files_layer = layer_db_df.select("layer_id", "dirs")
    layer_info = layer_db_df.filter(layer_db_df.selectExpr("explode(dirs) As structdirs").selectExpr(
        "explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*").sha256 == 'ad52c20d539762f3c44bf69b5162d694').take(1)
    print(layer_info)
    #regular_files = files.filter(files.sha256.isNotNull())
    

def save_cnt_bigger_1_fileinfo(spark, sc):
    #df_f = spark.read.parquet()
    #sha256 = cnt_bigger_1.select("sha256")
    cnt_bigger_1 = spark.read.parquet(output_uniq_files_cnts_bigger_1)
    sha256 = cnt_bigger_1.select("sha256")

    layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
    files = layer_db_df.selectExpr("explode(dirs) As structdirs").selectExpr(
        "explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
    regular_files = files.filter(files.sha256.isNotNull())

    cnt_bigger_1_df = regular_files.join(cnt_bigger_1, ['sha256'], 'leftsemi')
    cnt_bigger_1_df.show()
    
    cnt_bigger_1_df.write.save(cnt_bigger_1_fileinfo)    


def cumulative_sum_cal(spark, sc):
    df_f = spark.read.parquet(output_size_cnt_total_sum)
    df_f.describe('total_sum').show()
    print(df_f.where('total_sum = 675022836103').collect())
    #df_f = spark.read.parquet(output_unique_cnt_filename)
    #df_f.show()
    #df_f.describe('count').show()   
    #print(df_f.where('count = 53654306').collect())
    #cum = Window.orderBy('count').rowsBetween(-1, 1)
    #df_f.withColumn("cumsum", F.sum('count').over(cum)).show()


def save_sha256_sizes(spark, sc):
    cnt_bigger_1_df = spark.read.parquet(cnt_bigger_1_uniq)
    cnt_size = cnt_bigger_1_df.groupby('sha256').agg(collect_list('`file_info.stat_size`').alias('file_size'), F.sum('`file_info.stat_size`').alias('total_sum'), F.avg('`file_info.stat_size`').alias('avg'), F.size(collect_list('`file_info.stat_size`')).alias('cnt'))   
    cnt_size.show()
    cnt_size.write.save(output_size_cnt_total_sum)


def save_cnt_bigger_1_files(spark, sc):
    cnt_bigger_1 = spark.read.parquet(output_uniq_files_cnts_bigger_1)
    sha256 = cnt_bigger_1.select("sha256")
    
    layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
    files = layer_db_df.selectExpr("explode(dirs) As structdirs").selectExpr(
        "explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
    regular_files = files.filter(files.sha256.isNotNull())

    cnt_bigger_1_df = regular_files.join(cnt_bigger_1, ['sha256'], 'leftsemi')
    cnt_bigger_1_df.show()
    cnt_size = cnt_bigger_1_df.select(cnt_bigger_1_df.sha256, cnt_bigger_1_df.file_info.stat_size)
    cnt_size.write.save(cnt_bigger_1_uniq)
    """ #cnt_bigger_1_df.write.save()
    cnt_size = cnt_bigger_1_df.groupby(regular_files.sha256).agg(sum(cnt_bigger_1_df.file_info.stat_size), collect_list(cnt_bigger_1_df.file_info.stat_size).count())   
    cnt_size.show()
    cnt_size.write.save(output_uniq_cnts_sizes)"""
    

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
    # df = regular_files.filter(regular_files.type != "empty")
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
    #non_empty = regular_files.filter()
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

    return sc, spark


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
