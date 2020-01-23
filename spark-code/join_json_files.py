
# -*- coding: utf-8 -*-
#import os, json
#from pyspark import SparkContext, SparkConf
#from pyspark.sql import SparkSession
#from pyspark.sql.types import *
from analysis_library import *

HDFS_DIR = 'hdfs://hulk0:8020/'
#LAYER_JSONS_DIR1 = os.path.join(HDFS_DIR, 'nannan_2tb_hdd/layer_db_json')
#LAYER_JSONS_DIR2 = os.path.join(HDFS_DIR, '2tb_hdd_3/layer_db_json')
TEMP_DIR = os.path.join(HDFS_DIR, 'temp')
LOCAL_DIR = os.path.join(HDFS_DIR, 'local')
VAR_DIR = os.path.join(HDFS_DIR, 'var')

output_jsonfname = 'pop_image_json.parquet'

list_elem_num = 10000
#PARTITION_NUM = 1000

tempfilename = 'temp_json.lst'
input_fname = 'whole_layer_db_json.lst'

output_absfilename = os.path.join(VAR_DIR, output_jsonfname)#'2tb_hdd_json.parquet')#'nannan_2tb_json.parquet')#'layer_db_json_files.parquet')

all_json_absfilename = os.path.join(LOCAL_DIR, input_fname)#'nannan_2tb_json.lst')#'all_json_absfilename.lst')

output_files_fname = os.path.join(LOCAL_DIR, 'pop_image_files.parquet')
output_lfmap_fname = os.path.join(LOCAL_DIR, 'pop_image_layer_file_mapping.parquet')
output_lf_fname = os.path.join(LOCAL_DIR, 'pop_image_layer_info.parquet')

master = "spark://hulk0:7077"


def split_list(datalist):

    sublists = [datalist[x:x+list_elem_num] for x in range(0, len(datalist), list_elem_num)]

    #print(sublists)
    return sublists


def join_all_json_files(spark):
    DIR = LAYER_JSONS_DIR1
    spark.read.json(DIR).write.save(output_absfilename, format="parquet", mode='append')
    DIR = LAYER_JSONS_DIR2
    spark.read.json(DIR).write.save(output_absfilename, format="parquet", mode='append')


def join_json_files(absfilename_list, spark):

    #print(absfilename_list)
    sublists = split_list(absfilename_list)

    for sublist in sublists:
        #join_subset_json_files(sublist, spark)
	#join_subset_json_get_files(sublist, spark)
	#join_layer_file_mapping(sublist, spark)
	join_layer_info(sublist, spark)
        #break
        print("===========================>finished one sublist!!!!!!!!!")

def join_layer_info(sublist, spark):
    
    df = spark.read.json(sublist)
    #df.show()
    #df = df.select(extractfiles(df.layer_id, df.dirs.files).alias('files'))
    #df_f = df.select(F.explode('files').alias('fs'))

    #new_df = df_f.withColumn('layer_filename', col('fs.layer_filename'))
    #new_df = df_f.withColumn('layer_id', col('fs.layer_id'))
    #new_df = new_df.withColumn('filename', col('fs.filename'))
    #new_df = new_df.withColumn('digest', col('fs.digest'))

    new_df = df.select('layer_id', 'size', 'file_cnt')
    #df.show()
    new_df.write.save(output_lf_fname, format='parquet', mode='append')


def extract_file_digests_layer_id(layer_id, files):
    file_lst = []
    for f in files:
	filename = f['filename']
	if f['sha256']:
	    f_info = {
		#'layer_filename': os.path.basename(layer_filename),
		'layer_id': layer_id,
		'filename': filename,
		'digest': f['sha256']

	    }
	    file_lst.append(f_info)
    return file_lst


def join_layer_file_mapping(sublist, spark):

    extractfiles = F.udf(extract_file_digests_layer_id, ArrayType(StructType([
                    #StructField('layer_filename', StringType(), True),
                    StructField('layer_id', StringType(), True),
                    StructField('filename', StringType(), True),
                    StructField('digest', StringType(), True)])))

    df = spark.read.json(sublist)
    #df.show()
    df = df.select(extractfiles(df.layer_id, df.dirs.files).alias('files'))
    df_f = df.select(F.explode('files').alias('fs'))

    #new_df = df_f.withColumn('layer_filename', col('fs.layer_filename'))
    new_df = df_f.withColumn('layer_id', col('fs.layer_id'))
    new_df = new_df.withColumn('filename', col('fs.filename'))
    new_df = new_df.withColumn('digest', col('fs.digest'))

    new_df = new_df.select('layer_id', 'filename', 'digest')
    #new_df.show()
    new_df.write.save(output_lfmap_fname, format='parquet', mode='append')


def join_subset_json_get_files(sublist, spark):
    df = spark.read.json(sublist) 
    files = df.selectExpr("explode(dirs.files) As structfiles").selectExpr("structfiles.*")
    #files.show()
    files.write.save(output_files_fname, format="parquet", mode='append')    


def join_subset_json_files(sublist, spark):

    spark.read.json(sublist).coalesce(40000).write.save(output_absfilename, format="parquet", mode='append')


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
