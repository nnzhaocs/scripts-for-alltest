# -*- coding: utf-8 -*-
import os
#, json, logging
from itertools import chain
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, udf
from pyspark.sql import functions as F


HDFS_DIR = 'hdfs://hulk0:8020/'
MANIFESTS_DIR2 = os.path.join(HDFS_DIR, "/2tb_hdd/manifests")
MANIFESTS_DIR1 = os.path.join(HDFS_DIR, "/var/tmpmanifestdir/")
VAR_DIR = os.path.join(HDFS_DIR, 'var')
LOCAL_DIR = os.path.join(HDFS_DIR, 'local')

MANIFESTS_DIR = os.path.join(HDFS_DIR, "manifests")
LAYER_DB_JSON_DIR  = os.path.join(HDFS_DIR, "layer_db_jsons/")


pull_cnt_absfilename = os.path.join(LOCAL_DIR, "pull_cnt_with_filename.csv") #"_c3"
manifest_absfilename = os.path.join(MANIFESTS_DIR, "manifests_with_filename_with_layer_id_not_null.parquet")

layer_db_absfilename1 = os.path.join(LAYER_DB_JSON_DIR, "1g_big_json.parquet")
layer_db_absfilename2 = os.path.join(LAYER_DB_JSON_DIR, "2tb_hdd_json.parquet")
layer_db_absfilename3 = os.path.join(LAYER_DB_JSON_DIR, "nannan_2tb_json.parquet")

output_unique_cnt_not_empty_filename = os.path.join(LOCAL_DIR, 'output_unique_cnt_not_empty.parquet')
output_uniq_files_cnts_bigger_1_not_empty = os.path.join(LOCAL_DIR, 'output_uniq_files_cnts_bigger_1_not_empty.parquet')
output_absfilename = os.path.join(VAR_DIR, 'layer_file_mapping_filename2.parquet')


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

"""
230.2 M  /manifests/manifests_with_filename.parquet
181.7 G  /layer_db_jsons 
/local/image_pull_cnt.txt

/layer_db_jsons/1g_big_json.parquet
/layer_db_jsons/2tb_hdd_json.parquet
/layer_db_jsons/nannan_2tb_json.parquet
"""

#non_analyzed_layer_ids_lst = []
duplicate_files = []
#output_uniq_files_cnts_bigger_1_not_empty

def extract_dir_file_digests(dirs):

    file_lst = []

    for dir in dirs:
        if dir['files']:
            for f in dir['files']:
                if f['sha256']:
                    file_lst.append(f['sha256'])

    """file_cnt = len(set(file_lst))
    intersect_cnt = len(set(file_lst)&set(duplicate_files))
    return file_cnt*(0.1)/intersect_cnt#"""
    return list(set(file_lst)) 


def extract_file_digests(spark, sc):

    layer_db_df = spark.read.parquet(layer_db_absfilename2)#LAYER_DB_JSON_DIR)
    """
    file_df_lst = spark.read.parquet(output_uniq_files_cnts_bigger_1_not_empty)
    file_shas = file_df_lst.select('sha256').collect()
    file_lst = [str(i.value) for i in file_shas]
    for sha in file_lst:
	duplicate_files.append(sha)

    #layer_id_col = layer_db_df.select('layer_id')
    
    #df = layer_db_df.select('layer_id', F.explode('dirs').alias('dirs')).select('layer_id', F.explode('dirs.files').alias('files')).select('layer_id', 'files.sha256')#.groupby('layer_id').agg(F.collect_list('files.sha256'))
    #new_df_1.show()
    #new_df_1.printSchema()
    #new_df_2 = new_df_1.select("layer_id", "files.sha256")
    #new_df_3 = new_df_2.groupby('layer_id').agg(F.size(F.collect_set('sha256')).alias('uniq_file_cnt'))
    #new_df_3.show()"""
    extractfiles = udf(extract_dir_file_digests, ArrayType(StringType()))#FloatType())

    new_df = layer_db_df.select('layer_id', extractfiles(layer_db_df.dirs).alias('sha256s'))
    #new_df = df.select('layer_id', 'file_digests')
    #df = layer_db_df.selectExpr('layer_id', "explode(dirs) As structdirs").selectExpr('layer_id', "explode(structdirs.files) As structdirs_files").selectExpr('layer_id', "structdirs_files.sha256")
    #new_df = layer_db_df.groupby('layer_id').agg(F.explode(layer_db_df.dirs).alias('dirs')).groupby('layer_id').agg(F.explode('dirs.files').alias('files')).groupby('layer_id').agg('files.sha256')
    #new_df = df.filter(df.sha256.isNotNull())
    #new_df.show()
    #print(new_df_2.count())

    new_df.coalesce(40000).write.save(output_absfilename, mode='append')


def init_spark_cluster():
    """
    conf = SparkConf().setAppName('jsonsanalysis').setMaster(master)
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.appName("jsonsanalysis").config("spark.some.config.option", "some-value").master(
        master).getOrCreate()
    """
    conf = SparkConf() \
        .setAppName('jsonsanalysis') \
        .setMaster(master) \
        .set("spark.executor.cores", 5) \
        .set("spark.driver.memory", "15g") \
        .set("spark.executor.memory", "40g") \
        .set("spark.driver.maxResultSize", "10g")\
	.set("spark.network.timeout", "6000")
	#.set("spark.executor.heartbeatInterval", "10000000")\
    # .set("spark.sql.hive.filesourcePartitionFileCacheSize", "30g")
    sc = SparkContext(conf=conf)

    spark = SparkSession \
        .builder \
        .appName("jsonsanalysis") \
        .master(master) \
        .config(conf=conf) \
        .getOrCreate()

    return sc, spark


def main():
    sc, spark = init_spark_cluster()
    extract_file_digests(spark, sc)
    # join_all_json_files(spark)
    # absfilename_list = spark.read.text(all_json_absfilename).collect()
    # absfilenames = [str(i.value) for i in absfilename_list]
    # print absfilename_list
    # join_json_files(absfilenames, spark)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
