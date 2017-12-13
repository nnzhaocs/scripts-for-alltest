
# -*- coding: utf-8 -*-
import os
#, json, logging
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col


HDFS_DIR = 'hdfs://hulk0:8020/'
MANIFESTS_DIR2 = os.path.join(HDFS_DIR, "/2tb_hdd/manifests")
MANIFESTS_DIR1 = os.path.join(HDFS_DIR, "/var/tmpmanifestdir/")
VAR_DIR = os.path.join(HDFS_DIR, 'var')
LOCAL_DIR = os.path.join(HDFS_DIR, 'local')

MANIFESTS_DIR = os.path.join(HDFS_DIR, "manifests")
LAYER_DB_JSON_DIR  = os.path.join(HDFS_DIR, "layer_db_jsons/*")


pull_cnt_absfilename = os.path.join(LOCAL_DIR, "pull_cnt_with_filename.csv") #"_c3"
manifest_absfilename = os.path.join(LOCAL_DIR, "manifests_with_filename.parquet")

layer_db_absfilename1 = os.path.join(LAYER_DB_JSON_DIR, "1g_big_json.parquet")
layer_db_absfilename2 = os.path.join(LAYER_DB_JSON_DIR, "2tb_hdd_json.parquet")
layer_db_absfilename3 = os.path.join(LAYER_DB_JSON_DIR, "nannan_2tb_json.parquet")


output_absfilename = os.path.join(VAR_DIR, 'manifests.parquet')

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


def extract_columns(spark, sc):
    manifest_df = spark.read.load(manifest_absfilename)
    manifest_imagename_col = manifest_df.select("filename")
    manifest_layerids_col = manifest_df.select("layer_id")

    pull_df = spark.read.format("csv").load(pull_cnt_absfilename)
    pull_imagename_col = pull_df.select("_c3")
    pull_image_real_name_col = pull_df.select("_c2")

    layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
    layer_id_col = layer_db_df.select("layer_id")

    """manifest_layerids_col - layer_id_col = layer_id that has not been analyzed"""
    non_analyzed_layer = manifest_layerids_col.except(layer_id_col)

    """compare each manifest_layerids_col with layer_id that has not been analyzed"""

    manifest_df.where(col("layer_id").isin(non_analyzed_layer)).show()

    # manifest_layerids = manifest_layerids_col.collect()
    # manifest_layerids_lst = [str(i.value) for i in manifest_layerids]
    # spark.read.load(layer_db_absfilename1, layer_db_absfilename2, layer_db_absfilename3)
    # layer_db_
    # df.printSchema()
    # layer_id = df.select("digest")
    # layer_id.show()


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
    # join_all_json_files(spark)
    absfilename_list = spark.read.text(all_json_absfilename).collect()
    absfilenames = [str(i.value) for i in absfilename_list]
    #print absfilename_list
    join_json_files(absfilenames, spark)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'