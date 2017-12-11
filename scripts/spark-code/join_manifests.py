
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
MANIFESTS_DIR = os.path.join(HDFS_DIR, "/2tb_hdd/manifests")
MANIFESTS_DIR1 = os.path.join(HDFS_DIR, "/var/tmpmanifestdir/")
VAR_DIR = os.path.join(HDFS_DIR, 'var')
LOCAL_DIR = os.path.join(HDFS_DIR, 'local')

output_absfilename = os.path.join(VAR_DIR, 'manifests_with_filename.parquet')
output_absfilename_tmp = os.path.join(VAR_DIR, 'tmp_manifest.parquet')
""".set("spark.executor.cores", 5) \
    .set("spark.driver.memory", "10g") \
    .set("spark.executor.memory", "40g") \ 
"""

EXECUTOR_CORES = 5
EXECUTOR_MEMORY = "40g"
DRIVER_MEMORY = "10g"

list_elem_num = 1000

master = "spark://hulk0:7077"

all_json_absfilename = os.path.join(LOCAL_DIR, 'manifest.lst')
all_json_absfilename_tmp = os.path.join(VAR_DIR, 'tmp_manifest.lst')

def split_list(datalist):

    sublists = [datalist[x:x+list_elem_num] for x in range(0, len(datalist), list_elem_num)]

    print(sublists)
    return sublists


def join_manifests(absfilename_list, output_absfilename, sc, spark):

    sublists = split_list(absfilename_list)

    for sublist in sublists:
	#if not sublist:
	#    continue
        #filename_df = spark.read.json(sublist).select(sourceFile(input_file_name()))#.write.save(output_absfilename, format="parquet", mode='append')
        #spark.read.json(sublist).select('schemaVersion'!=2).write.save(output_absfilename, format="parquet", mode='append')
        spark.read.json(sublist, multiLine=True).write.save(output_absfilename, format="parquet", mode='append')

        #spark.read.json("tmp.json").select().show()

        # json_data_rdd = sc.wholeTextFiles(input_dirname).coalesce(4000).map(parse_manifest)
        #spark.read.json(json_data_rdd, multiLine=True).write.save(output_absfilename, format="parquet", mode='append')
        #json_data_rdd.toDF().write.save(output_absfilename, format="parquet", mode='append')


def parse_and_store_manifest(absfilename_list, output_absfilename, sc, spark):

    sublists = split_list(absfilename_list)

    # json_data_rdd = sc.wholeTextFiles(input_dirname).map(parse_manifest)
    # join_json_data_rdd = json_data_rdd.join()
    extract_filename = udf(lambda s: os.path.basename(s))

    for sublist in sublists:
        blomSum = True
        digest = True
        df = spark.read.json(sublist, multiLine=True)
    # df =
    # spark.read.json(input_dirname, multiLine=True).write.save(output_absfilename, format="parquet", mode='append')
    # configs = df.select('config.digest')
    # configs.show()
    # configs.select('')
        try:
            df.select('fsLayers.blobSum')
        except:
            blomSum = False
            info = df.select('schemaVersion', 'layers.digest', input_file_name())
            out_df = info.select(extract_filename("input_file_name()").alias("filename"), 'schemaVersion', 'digest')

        try:
            df.select('layers.digest')
        except:
            digest = False
            info = df.select('schemaVersion', 'fsLayers.blobSum', input_file_name())  # .show()
            out_df = info.select(extract_filename("input_file_name()").alias("filename"), 'schemaVersion', 'blobSum')

        if blomSum and digest:
            info = df.select('schemaVersion', 'fsLayers.blobSum', 'layers.digest', input_file_name())#.show()
            out_df = info.select(extract_filename("input_file_name()").alias("filename"), 'schemaVersion', 'blobSum', 'digest')

        out_df.write.save(output_absfilename, format="parquet", mode='append')
        # info.select("filename", input_file_name().split('/')[-1])
        # info.input_file_name().show()
        # info.withColumn('filename', os.path.basename(info.input_file_name()))
        # df.toDF()
        # df = json_data_rdd.toDF()
        # out_df.printSchema()
        # out_df.show()
        # info.withColumn('filename', os.path.basename(info.input_file_name()))
        # df =
        # df.write.save(output_absfilename, format="parquet", mode='append')
        # df = spark.read.json(input_dirname)
        # df.printSchema()
        # df.select(df.schemaVersion !=2).show()#.


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
        .set("spark.executor.cores", EXECUTOR_CORES)\
        .set("spark.driver.memory", DRIVER_MEMORY)\
        .set("spark.executor.memory", EXECUTOR_MEMORY)\
	.set("spark.sql.parquet.mergeSchema", True)
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
    #fmt = "%(funcName)s():%(lineno)i: %(message)s %(levelname)s"
    #logging.basicConfig(level=logging.DEBUG, format=fmt)

    sc, spark = init_spark_cluster()

    absfilename_list = spark.read.text(all_json_absfilename).collect()
    absfilenames = [str(i.value) for i in absfilename_list]

    parse_and_store_manifest(absfilenames, output_absfilename, sc, spark)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
