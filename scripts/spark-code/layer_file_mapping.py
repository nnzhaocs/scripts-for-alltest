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

#non_analyzed_layer_ids_lst = []
# duplicate_files = []
#output_uniq_files_cnts_bigger_1_not_empty

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

    # layer_db_df = spark.read.parquet(layer_db_absfilename2)#LAYER_DB_JSON_DIR)
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

    extractfiles = udf(extract_dir_file_digests, ArrayType(StructType([StructField('filename', StringType(), True), StructField('digest', StringType(), True)])))

    sublists = split_list(absfilename_list)

    for sublist in sublists:
        # join_subset_json_files(sublist, spark)
        # rawData = sc.wholeTextFiles(LAYER_JSONS_DIR1).map(lambda x: json.loads(x[1])).flatMap(parse_layer_json)
        layer_db_df = spark.read.json('/var/1gb_json.json', multiLine=True)#sublist)
	layer_db_df.printSchema()
        new_df = layer_db_df.select('layer_id', extractfiles(layer_db_df.dirs).alias('files'))
        new_df.coalesce(400).write.save(output_absfilename3, mode='append')
	break
        print("===========================>finished one sublist!!!!!!!!!")

    #new_df = df.select('layer_id', 'file_digests')
    #df = layer_db_df.selectExpr('layer_id', "explode(dirs) As structdirs").selectExpr('layer_id', "explode(structdirs.files) As structdirs_files").selectExpr('layer_id', "structdirs_files.sha256")
    #new_df = layer_db_df.groupby('layer_id').agg(F.explode(layer_db_df.dirs).alias('dirs')).groupby('layer_id').agg(F.explode('dirs.files').alias('files')).groupby('layer_id').agg('files.sha256')
    #new_df = df.filter(df.sha256.isNotNull())
    #new_df.show()
    #print(new_df_2.count())


def split_list(datalist):

    sublists = [datalist[x:x+list_elem_num] for x in range(0, len(datalist), list_elem_num)]

    #print(sublists)
    return sublists


# def join_all_json_files(spark):
#     DIR = LAYER_JSONS_DIR1
#     spark.read.json(DIR).write.save(output_absfilename, format="parquet", mode='append')
#     DIR = LAYER_JSONS_DIR2
#     spark.read.json(DIR).write.save(output_absfilename, format="parquet", mode='append')


# def join_json_files(absfilename_list, spark):

    #print(absfilename_list)

# def join_subset_json_files(sublist, spark):
#
#     spark.read.json(sublist).coalesce(40000).write.save(output_absfilename, format="parquet", mode='append')


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
    absfilename_list = spark.read.text(all_json_absfilename).collect()
    absfilenames = [str(i.value) for i in absfilename_list]

    extract_file_digests(absfilenames, spark)
    # join_all_json_files(spark)
    # absfilename_list = spark.read.text(all_json_absfilename).collect()
    # absfilenames = [str(i.value) for i in absfilename_list]
    # print absfilename_list
    # join_json_files(absfilenames, spark)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
