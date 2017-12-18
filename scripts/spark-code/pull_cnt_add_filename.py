# -*- coding: utf-8 -*-
import os
#, json, logging
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import udf

HDFS_DIR = 'hdfs://hulk0:8020/'
MANIFESTS_DIR2 = os.path.join(HDFS_DIR, "/2tb_hdd/manifests")
MANIFESTS_DIR1 = os.path.join(HDFS_DIR, "/var/tmpmanifestdir/")
VAR_DIR = os.path.join(HDFS_DIR, 'var')
LOCAL_DIR = os.path.join(HDFS_DIR, 'local')

MANIFESTS_DIR = os.path.join(HDFS_DIR, "manifests")
LAYER_DB_JSON_DIR = os.path.join(HDFS_DIR, "layer_db_jsons")

pull_cnt_absfilename = os.path.join(LOCAL_DIR, "image_pull_cnt.txt")
manifest_absfilename = os.path.join(LOCAL_DIR, "manifests_with_filename.parquet")

layer_db_absfilename1 = os.path.join(MANIFESTS_DIR, "1g_big_json.parquet")
layer_db_absfilename2 = os.path.join(MANIFESTS_DIR, "2tb_hdd_json.parquet")
layer_db_absfilename3 = os.path.join(MANIFESTS_DIR, "nannan_2tb_json.parquet")

output_absfilename = os.path.join(VAR_DIR, 'pull_cnt_with_filename.csv')

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


def pull_cnt_add_filename(spark, sc):
    # manifest_df = spark.read.load(manifest_absfilename)
    df = spark.read.format("csv").load(pull_cnt_absfilename)
    # df.withColumnRenamed('_c2', 'filename')
    convert_imagenames = udf(lambda s: s.replace("/", "-") + '-' + 'latest')
    out_df = df.select('_c0', '_c1', '_c2')
    new_df = out_df.withColumn('_c3', convert_imagenames(df._c2))
    new_df.show()
    new_df.write.csv(output_absfilename)

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
    conf = SparkConf() \
        .setAppName('jsonsanalysis') \
        .setMaster(master) \
        .set("spark.executor.cores", 5) \
        .set("spark.driver.memory", "10g") \
        .set("spark.executor.memory", "40g") \
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
    # join_all_json_files(spark)
    # absfilename_list = spark.read.text(all_json_absfilename).collect()
    # absfilenames = [str(i.value) for i in absfilename_list]
    # print absfilename_list
    pull_cnt_add_filename(spark, sc)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
