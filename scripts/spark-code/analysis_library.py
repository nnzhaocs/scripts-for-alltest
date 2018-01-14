# -*- coding: utf-8 -*-
import os
#, json, logging
# from itertools import chain
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

REDUNDANT_FILE_ANALYSIS_DIR = os.path.join(HDFS_DIR, 'redundant_file_analysis')
REDUNDANT_LAYER_ANALYSIS_DIR = os.path.join(HDFS_DIR, 'redundant_layer_analysis')
REDUNDANT_IMAGE_ANALYSIS_DIR = os.path.join(HDFS_DIR, 'redundant_image_analysis')
REDUNDANT_DIR_ANALYSIS_DIR = os.path.join(HDFS_DIR, 'redundant_dir_analysis')

MANIFESTS_DIR = os.path.join(HDFS_DIR, "manifests")
LAYER_DB_JSON_DIR  = os.path.join(HDFS_DIR, "layer_db_jsons/*")
LAYER_FILE_MAPPING_DIR = os.path.join(HDFS_DIR, "layer_file_mapping/*")


# pull_cnt_absfilename = os.path.join(LOCAL_DIR, "pull_cnt_with_filename.csv") #"_c3"
# manifest_absfilename = os.path.join(MANIFESTS_DIR, "manifests_with_filename_with_layer_id_not_null.parquet")

layer_db_absfilename1 = os.path.join(LAYER_DB_JSON_DIR, "1g_big_json.parquet")
layer_db_absfilename2 = os.path.join(LAYER_DB_JSON_DIR, "2tb_hdd_json.parquet")
layer_db_absfilename3 = os.path.join(LAYER_DB_JSON_DIR, "nannan_2tb_json.parquet")

layer_file_mapping1 = os.path.join(VAR_DIR, 'layer_file_mapping_nannan_2tb_hdd.parquet')
layer_file_mapping2 = os.path.join(VAR_DIR, 'layer_file_mapping_2tb_hdd.parquet')
layer_file_mapping3 = os.path.join(VAR_DIR, 'layer_file_mapping_1gb_layer.parquet')

unique_file_info = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_file_info.parquet')
unique_size_cnt_total_sum = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_size_cnt_total_sum.parquet')
layer_basic_info = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_basic_info.parquet')

""".set("spark.executor.cores", 5) \
    .set("spark.driver.memory", "10g") \
    .set("spark.executor.memory", "40g") \ 
"""

EXECUTOR_CORES = 5
EXECUTOR_MEMORY = "40g"
DRIVER_MEMORY = "10g"

# list_elem_num = 10000

master = "spark://hulk0:7077"

"""==========================> files"""


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