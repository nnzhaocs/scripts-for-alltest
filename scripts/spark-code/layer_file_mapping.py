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

LAYER_JSONS_DIR1 = os.path.join(HDFS_DIR, 'nannan_2tb_hdd/layer_db_json')
LAYER_JSONS_DIR2 = os.path.join(HDFS_DIR, '2tb_hdd_3/layer_db_json')
TEMP_DIR = os.path.join(HDFS_DIR, 'temp')

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
output_absfilename1 = os.path.join(VAR_DIR, 'layer_file_mapping_nannan_2tb_hdd.parquet')
output_absfilename2 = os.path.join(VAR_DIR, 'layer_file_mapping_2tb_hdd.parquet')
output_absfilename3 = os.path.join(VAR_DIR, 'layer_file_mapping_1gb_layer.parquet')
""".set("spark.executor.cores", 5) \
    .set("spark.driver.memory", "10g") \
    .set("spark.executor.memory", "40g") \ 
"""


EXECUTOR_CORES = 5
EXECUTOR_MEMORY = "40g"
DRIVER_MEMORY = "10g"

list_elem_num = 10000

master = "spark://hulk0:7077"

# all_json_absfilename = os.path.join(LOCAL_DIR, 'manifest.lst')
all_json_absfilename = os.path.join(VAR_DIR, '1gb_json.lst')#'1gb_layer_json.lst')#'nannan_2tb_json.lst')#'2tb_hdd_json.lst')#'nannan_2tb_json.lst')#'all_json_absfilename.lst')

"""
230.2 M  /manifests/manifests_with_filename.parquet
181.7 G  /layer_db_jsons 
/local/image_pull_cnt.txt

/layer_db_jsons/1g_big_json.parquet
/layer_db_jsons/2tb_hdd_json.parquet
/layer_db_jsons/nannan_2tb_json.parquet
"""

def extract_dir_file_digests(dirs):

    file_lst = []

    for dir in dirs:
        dirname = dir['subdir']
	if not dir['subdir']:
	    dirname = '/'
        if dir['files']:
            for f in dir['files']:
		filename = f['filename']
                if f['sha256']:
                    #f_info = {}
		    f_info = {
			'filename': os.path.join(dirname, filename),
			'digest': f['sha256']
		    }
                    file_lst.append(f_info)#f['sha256'])

    """file_cnt = len(set(file_lst))
    intersect_cnt = len(set(file_lst)&set(duplicate_files))
    return file_cnt*(0.1)/intersect_cnt#"""
    return file_lst


def extract_file_digests(absfilename_list, spark):

    extractfiles = udf(extract_dir_file_digests, ArrayType(StructType([StructField('filename', StringType(), True), StructField('digest', StringType(), True)])))

    sublists = split_list(absfilename_list)

    for sublist in sublists:
        """=======================================> modify here"""
        layer_db_df = spark.read.json('/var/1gb_json.json', multiLine=True)#sublist)
	layer_db_df.printSchema()
        new_df = layer_db_df.select('layer_id', extractfiles(layer_db_df.dirs).alias('files'))
        new_df.coalesce(400).write.save(output_absfilename3, mode='append')
	break
        print("===========================>finished one sublist!!!!!!!!!")


def split_list(datalist):

    sublists = [datalist[x:x+list_elem_num] for x in range(0, len(datalist), list_elem_num)]
    return sublists


def init_spark_cluster():

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
    absfilename_list = spark.read.text(all_json_absfilename).collect()
    absfilenames = [str(i.value) for i in absfilename_list]

    extract_file_digests(absfilenames, spark)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
