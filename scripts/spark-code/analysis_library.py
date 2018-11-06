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
#import re
from get_file_type import *


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
LAYER_DB_JSON_DIR1  = os.path.join(HDFS_DIR, "layer_db_jsons/")

LAYER_FILE_MAPPING_DIR = os.path.join(HDFS_DIR, "layer_file_mapping/*")
LAYER_FILE_MAPPING_DIR1 = os.path.join(HDFS_DIR, "layer_file_mapping/")


# pull_cnt_absfilename = os.path.join(LOCAL_DIR, "pull_cnt_with_filename.csv") #"_c3"
# manifest_absfilename = os.path.join(MANIFESTS_DIR, "manifests_with_filename_with_layer_id_not_null.parquet")

layer_db_absfilename1 = os.path.join(LAYER_DB_JSON_DIR1, "1g_big_json.parquet")
layer_db_absfilename2 = os.path.join(LAYER_DB_JSON_DIR1, "2tb_hdd_json.parquet")
layer_db_absfilename3 = os.path.join(LAYER_DB_JSON_DIR1, "nannan_2tb_json.parquet")

layer_file_mapping1 = os.path.join(LAYER_FILE_MAPPING_DIR1, 'layer_file_mapping_nannan_2tb_hdd.parquet')
layer_file_mapping2 = os.path.join(LAYER_FILE_MAPPING_DIR1, 'layer_file_mapping_2tb_hdd.parquet')
layer_file_mapping3 = os.path.join(LAYER_FILE_MAPPING_DIR1, 'layer_file_mapping_1gb_layer.parquet')

unique_file_layer_mapping = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'unique_file_layer_mapping.parquet')

unique_file_basic_info = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_file_basic_info.parquet')
unique_size_cnt_total_sum = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_size_cnt_total_sum.parquet')
layer_basic_info1 = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_basic_info.parquet1')
layer_basic_info2 = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_basic_info.parquet2')
layer_basic_info3 = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_basic_info.parquet3')
image_basic_info = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_basic_info.parquet3')

uniq_1_copy_file_info = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'uniq_1_copy_file_info.parquet')
uniq_1_copy_ftype_by_cap = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR,'uniq_1_copy_ftype_by_cap.parquet')

pop_image_layer_db_json = os.path.join(VAR_DIR, 'pop_image_json.parquet')

pop_image_files = os.path.join(LOCAL_DIR, 'pop_image_files.parquet')
pop_image_lfmap = os.path.join(LOCAL_DIR, 'pop_image_layer_file_mapping.parquet')
pop_image_linfo = os.path.join(LOCAL_DIR, 'pop_image_layer_info.parquet')
pop_image_ilmap = os.path.join(LOCAL_DIR, 'pop_image_image_layer_mapping.parquet') 
pop_image_all_linfo = os.path.join(LOCAL_DIR, 'pop_image_layer_file_all_mapping.parquet')
pop_image_uniq_file_address = os.path.join(LOCAL_DIR, 'pop_image_uniq_file_address.parquet')#.csv

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
        .set("spark.executor.instances", 1)\
        .set("spark.driver.memory", "10g")\
        .set("spark.executor.memory", "40g")\
        .set("spark.driver.maxResultSize", "10g")
        #.set('spark.dynamicAllocation.enabled', False)\
	#.set("spark.sql.shuffle.partitions", 4000)
        #.set("spark.sql.hive.filesourcePartitionFileCacheSize", "30g")
    sc = SparkContext(conf = conf)

    spark = SparkSession \
        .builder \
        .appName("jsonsanalysis")\
        .master(master)\
        .config(conf = conf)\
        .getOrCreate()

    sc.addFile("./get_file_type.py")

    return sc, spark
