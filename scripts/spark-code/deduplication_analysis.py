
# -*- coding: utf-8 -*-
import os
#, json, logging
from itertools import chain
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import col, collect_list, size
from pyspark.sql import functions as F
from collections import Counter as mset


HDFS_DIR = 'hdfs://hulk0:8020/'
MANIFESTS_DIR2 = os.path.join(HDFS_DIR, "/2tb_hdd/manifests")
MANIFESTS_DIR1 = os.path.join(HDFS_DIR, "/var/tmpmanifestdir/")
VAR_DIR = os.path.join(HDFS_DIR, 'var')
LOCAL_DIR = os.path.join(HDFS_DIR, 'local')
TABLE_DIR = os.path.join(HDFS_DIR, 'table')

MANIFESTS_DIR = os.path.join(HDFS_DIR, "manifests")
LAYER_DB_JSON_DIR  = os.path.join(HDFS_DIR, "layer_db_jsons/*")
LAYER_FILE_MAPPING_DIR = os.path.join(HDFS_DIR, "layer_file_mapping/*")


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

redundant_ratio_in_layers = os.path.join(LOCAL_DIR, 'redundant_ratio_in_layers.parquet')
shared_ratio_in_layers = os.path.join(LOCAL_DIR, 'shared_ratio_in_layers')

explode_fdigest_layers = os.path.join(LOCAL_DIR, 'explode_fdigest_layers.parquet')

layer_file_rank = os.path.join(LOCAL_DIR, 'layer_file_rank.parquet')

share_layer_file = os.path.join(TABLE_DIR, 'share_layer_file.parquet')
uniq_layer_file = os.path.join(TABLE_DIR, 'uniq_layer_file.parquet')
shared_ratio_layer = os.path.join(TABLE_DIR, 'shared_ratio_layer.parquet')
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
    #describe_file(spark, sc)
    #save_unqiue_files_cnts(spark, sc)
    #calculate_redundant_files_in_layers(spark, sc)
    #save_cnt_bigger_1_files(spark, sc)
    #save_sha256_sizes(spark, sc)
    #cumulative_sum_cal(spark, sc)
    #find_file(spark, sc)
    #find_file_in_layer(spark, sc)
    #save_cnt_bigger_1_fileinfo(spark, sc)
    #calculate_shared_files_in_layers(spark, sc)
    #get_uniq_layer_file(spark, sc)
    #find_file_path(spark, sc)
    calculate_uniq_files_layerset(spark, sc)

# def get_layer_shared_file_proportion(spark, sc):
#     output_unique_filename

shared_lst = []
def get_layer_shares(lst):
    if not lst:
        return 0.0
    intersection = mset(lst) & mset(shared_lst)
    return len(list(intersection))*(0.1)/len(lst)


def save_uniq_layer_file(spark, sc):
    df_f = spark.read.parquet(uniq_layer_file)
    df = df_f.select('layer_id', 'filename')
    df.write.csv("/var/layer_uniq_filename.csv")


def find_file_path(spark, sc):
    df_f = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    df_f.filter(df_f.digest == '660efd2a328301f3beec03984a9b421f').show(20, False)


def calculate_shared_files_in_layers(spark, sc):
    df_f = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    df = df_f.groupby('layer_id').agg(size(collect_list('digest')).alias('total_file_cnt'))
    
    """count"""
    share_f = spark.read.parquet(share_layer_file)
    df_2 = share_f.groupby('layer_id').agg(size(collect_list('digest')).alias('total_shared_cnt'))

    new_df = df.join(df_2, 'layer_id', 'outer')
    df = new_df.withColumn("shared_ratio", (F.col('total_shared_cnt')/F.col('total_file_cnt')))
    #df.show()
    df.write.save(shared_ratio_layer)


def get_shared_files_in_layers(spark, sc):
    share_df = spark.read.parquet(output_uniq_files_cnts_bigger_1)
    shared_files = share_df.select(share_df.sha256.alias('digest'))
    shared_files.show()

    df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    df.show()
    share = df.join(shared_files, ['digest'], 'leftsemi')
    
    share.show()
    share.write.save(share_layer_file)


def calculate_uniq_files_layerset(spark, sc):
    df = spark.read.parquet(uniq_layer_file)
    layer_id = df.select('layer_id')
    print(layer_id.dropDuplicates().count())


def calculate_uniq_files_in_layers(spark, sc):
    """collect all unique file sha256 output_uniq_files_cnts_bigger_1"""
    share_df = spark.read.parquet(output_uniq_files_cnts_bigger_1)
    shared_files = share_df.select(share_df.sha256.alias('digest'))#.collect()
    #shared_file_lst = [str(i.value) for i in shared_files]
    shared_files.show()   
    #lst = sc.broadcast(shared_file_lst)
    #for file_sha in shared_file_lst:
    #	shared_lst.append(file_sha)
    """calculate """
    df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    #digests = df.select('digest')
    df.show()
    diff = df.join(shared_files, ['digest'], 'left_anti')
    diff.show(20, False)
    #print("======================> count = %d", diff.count())
    #diff = diff.select('layer_id', 'filename')
    diff.write.save(uniq_layer_file)
    #new_df = df.withColumn('is_shared', df.digest.isin(lst.value))
    #func = F.udf(get_layer_shares, FloatType())
    #func = F.udf(rank_layer_fcnt)
    #new_df = df.select('layer_id', F.explode('files.digest').alias('digest'))
    #new_df = df.withColumn('shared_ratio_in_layers', func(df.digest))
    #cum = Window.partitionBy('layer_id').rowsBetween(-1, 1)
    #new_df = df.withColumn("file_cnt", F.size('digest'))
    #new_df = df.withColumn('fdigest', F.explode('digest'))
    #new_df.show()
    #new_df.describe('file_cnt').show()
    #df_f = new_df.withColumn("rank", func('file_cnt'))
    #df_f.show()
    #df_f.write.partitonBy('rank').parquet(layer_file_rank)
    #new_df.coalesce(4000).write.save(explode_fdigest_layers)#shared_ratio_in_layers)


def describe_file(spark, sc):
    df_f = spark.read.parquet(redundant_ratio_in_layers)
    df_f.describe('redundant_ratio_in_layers').show()


def get_layer_duplicates(lst):
    if not lst:
	return 0.0
    return len(set(lst))*(0.1)/len(lst)


def calculate_redundant_files_in_layers(spark, sc):
    df_f = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    #df = df_f.select('sha256', F.sum(collec'file_size').alias('total_sum'), F.size('file_size').alias('cnt'), F.avg('file_size').alias('avg'))
    df_f.printSchema()
    #df_f.show()
    df = df_f.select('layer_id', 'files.digest')
    func = F.udf(get_layer_duplicates, FloatType())
    new_df = df.withColumn('redundant_ratio_in_layers', func(df.digest))
    new_df.show()
    new_df.write.save(redundant_ratio_in_layers)


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
        .set("spark.driver.maxResultSize", "10g")\
	.set("spark.local.dir", "/home/nannan/spark/tmp")
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
