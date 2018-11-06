
# -*- coding: utf-8 -*-
import os, json, ijson
from pyspark import SparkContext, SparkConf
from IPython.display import clear_output, Image, display
from pyspark.sql import SparkSession
from pyspark.sql import Row, SQLContext
#from pyspark.sql import SQLContext as sqlContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
import collections
#import SQLContext
from pyspark.sql.functions import col, udf
from pyspark.sql.window import Window
import time
import datetime

#spark.read.json(“/path/to/myDir”)
HDFS_DIR = 'hdfs://hulk0:8020/'
TRACE_DIR = os.path.join(HDFS_DIR, 'traces')
TRACE_PARQ_DIR = os.path.join(HDFS_DIR, 'traces_parquet')
OUTPUT_CSV_DIR = os.path.join(HDFS_DIR, 'output_csv')
TEMP_DIR = os.path.join(HDFS_DIR, 'temp')
LOCAL_DIR = os.path.join(HDFS_DIR, 'local')
#tmp_filename = 'sha256-d33e7d48b1b4d2902966200e28f31f642d329bf59c8410c8559dd2c059bf28fa-1501273930.87.json'
#tmp_filename = 'sha256-0000086583dd63d03a9e36820a3e8c8b85ac99d3ff19336c92ee793107e208eb-1500673662.87.json'

#temp_filename1 = '/2tb_hdd_3/layer_db_json/sha256-06cc1ee2359e4ceb90f832c8c86c06dd82705ed7eefe0a62948c4cb6225c7969-1505184549.14.json'

#temp_filename = '/var/1gb_json.json'


master = "spark://hulk0:7077"

#conf =  SparkConf().setAppName('jsonsanalysis').setMaster(master)
#sc = SparkContext(conf = conf)

#master = "spark://hulk0:7077"
#spark = SparkSession.builder.appName("jsonsanalysis").config("spark.some.config.option", "some-value").master(master).getOrCreate()

conf =  SparkConf()\
        .setAppName('jsonsanalysis')\
        .setMaster(master)\
        .set("spark.executor.cores", 5)\
        .set("spark.driver.memory", "10g")\
        .set("spark.executor.memory", "40g")\
	.set("spark.driver.maxResultSize", "2g")
        #.set("spark.sql.hive.filesourcePartitionFileCacheSize", "30g")
sc = SparkContext(conf = conf)

spark = SparkSession \
        .builder \
        .appName("jsonsanalysis")\
        .master(master)\
        .config(conf = conf)\
        .getOrCreate()

sqlContext = SQLContext(spark.sparkContext)

def extract_usrrepolayer(uri):
    usrname = uri.split('/')[1]
    repo_name = uri.split('/')[2]
    layer_id = ''
    manifest = ''
    repo_name = usrname+'/'+repo_name
    if 'blob' in uri:
        layer_id = uri.rsplit('/', 1)[1]
    elif 'manifest' in uri:
        manifest = uri.rsplit('/', 1)[1]
    info = {
                'usrname': usrname,
                'repo_name': repo_name,
                'layer_id': layer_id,
                'manifest': manifest
            }
    return info


extracturis = udf(extract_usrrepolayer, StructType([
                StructField('usrname', StringType(), True),
                StructField('repo_name', StringType(), True),
                StructField('layer_id', StringType(), True),
                StructField('manifest', StringType(), True)
    ]))

def save_traces_2_parquet(filename):
    df = spark.read.json(os.path.join(TRACE_DIR, filename), multiLine=True)#.coalesce(400).write.save(CACHE_ANALYSIS_PARQ_DIR, format="parquet", mode='append')

    df.printSchema()

#df.show(20, True)

    df = df.select(col("`http.request.duration`").alias('duration'), col("`http.request.method`").alias('method'), col("`http.request.uri`").alias('uri'), col("`http.response.written`").alias('size'), 'timestamp')

    df.printSchema()

    df = df.select(extracturis('uri').alias('repoinfo'), 'uri', 'duration', 'method', 'size', 'timestamp')
#df_f = df.select(F.explode('repoinfo').alias('repo_info'), 'uri')
    df.printSchema()
#new_df = df_f.withColumn('usrname', col('repo_info.usrname'))
#new_df = new_df.withColumn('reponame', col('repo_info.repo_name'))
#new_df = new_df.withColumn('layer_id', col('repo_info.layer_id'))
#new_df = new_df.withColumn('digest', col('fs.digest'))
#new_df = new_df.select('uri', 'layer_id', 'reponame', 'usrname')
#new_df.coalesce(400).write.save(output_absfilename2, mode='append')
#break
#print("===========================>finished one sublist!!!!!!!!!")
#new_df.printSchema()
#new_df.show()
    df.coalesce(400).write.save(os.path.join(TRACE_PARQ_DIR, filename+'.parquet'), format="parquet", mode='append')


def save_repo_blob_GP_cnt(filename):
    df = spark.read.parquet(os.path.join(TRACE_PARQ_DIR, 'dal_09.parquet'))
    df.printSchema()
    df.show()
    #df_f.filter(df_f.digest == '660efd2a328301f3beec03984a9b421f').show(20, False)
    blob_df = df.filter(df.repoinfo.layer_id != '')
    #regular_files = files.filter(files.sha256.isNotNull())
    #new_df = df.groupby()
    #print("========> ", df.count(), blob_df.count())
    #cnt_bigger_1_df.groupby('sha256').agg(collect_list('`file_info.stat_size`').alias('file_size'), F.sum('`file_info.stat_size`').alias('total_sum'), F.avg('`file_info.stat_size`').alias('avg'), F.size(collect_list('`file_info.stat_size`')).alias('cnt'))
    GET_blob = blob_df.filter(blob_df.method == 'GET')
    PUT_blob = blob_df.filter(blob_df.method == 'PUT')
    #print("=========>", GET_blob.count(), PUT_blob.count())
    #new_df = blob_df.groupby('repoinfo.repo_name').agg(F.size('repoinfo.'))
    GET_blob_repo = GET_blob.groupby('repoinfo.repo_name').agg(F.size(F.collect_list('repoinfo')).alias('GET_cnt'))
    PUT_blob_repo = PUT_blob.groupby('repoinfo.repo_name').agg(F.size(F.collect_list('repoinfo')).alias('PUT_cnt'))
    #GET_blob_repo.show()
    new_df = GET_blob_repo.join(PUT_blob_repo, 'repo_name', 'full')
    print("========>", new_df.count(), GET_blob_repo.count(), PUT_blob_repo.count())

    new_df = new_df.na.fill(0)#.show()
    new_df.show()

    new_df.coalesce(400).write.save(os.path.join(TRACE_PARQ_DIR, 'repo_blob_GP_cnt.parquet'), format='parquet', mode='append')


def export_parquet_2_csv(filename):
    df = spark.read.parquet(os.path.join(TRACE_PARQ_DIR, filename))
    df.repartition(1).write.csv(os.path.join(OUTPUT_CSV_DIR , filename+'.csv'))


def save_usr_blob_GP_cnt(filename):
    df = spark.read.parquet(os.path.join(TRACE_PARQ_DIR, 'dal_09.parquet'))
    blob_df = df.filter(df.repoinfo.layer_id != '')
    GET_blob = blob_df.filter(blob_df.method == 'GET')
    PUT_blob = blob_df.filter(blob_df.method == 'PUT')
    GET_blob_usr = GET_blob.groupby('repoinfo.usrname').agg(F.size(F.collect_list('repoinfo')).alias('GET_cnt'))
    PUT_blob_usr = PUT_blob.groupby('repoinfo.usrname').agg(F.size(F.collect_list('repoinfo')).alias('PUT_cnt'))

    new_df = GET_blob_usr.join(PUT_blob_usr, 'usrname', 'full')
    print("========>", new_df.count(), GET_blob_usr.count(), PUT_blob_usr.count())

    new_df = new_df.na.fill(0)#.show()
    new_df.show()

    new_df.coalesce(400).write.save(os.path.join(TRACE_PARQ_DIR, 'usr_blob_GP_cnt.parquet'), format='parquet', mode='append')

def export_time_method_repoinfo_csv():
    df = spark.read.parquet(os.path.join(TRACE_PARQ_DIR, 'dal_09.parquet'))
    #df.show()
    df = df.select('timestamp', 'method', 'repoinfo.layer_id', 'repoinfo.manifest', 'repoinfo.repo_name', 'repoinfo.usrname')
    df.show()
    df.coalesce(400).write.save(os.path.join(TRACE_PARQ_DIR, 'time_method_repoinfo.parquet'), format='parquet', mode='append')
    export_parquet_2_csv('time_method_repoinfo.parquet')


#def manifest_empty_ops()

def save_repo_partition_cvs(filename, colname, outputfname):
    df = spark.read.parquet(os.path.join(TRACE_PARQ_DIR, filename))
    df = df.repartition(colname)
    df.coalesce(400).write.save(os.path.join(TRACE_PARQ_DIR, outputfname), format='parquet', mode='append')
    export_parquet_2_csv(outputfname)

#save_repo_partition_cvs('time_method_repoinfo.parquet', 'usrname', 'usr_partition.parquet')


# usr active_time(1 hr/min) pulls repos time-interval
#
"""
windowSpec = \
        Window
        .partitionBy(df['category']) \
        .orderBy(df['revenue'].desc()) \
        .rangeBetween(-sys.maxsize, sys.maxsize)
dataFrame = sqlContext.table("productRevenue")
revenue_difference = \
        (func.max(dataFrame['revenue']).over(windowSpec) - dataFrame['revenue'])
ataFrame.select(
                                                  dataFrame['product'],
                                                    dataFrame['category'],
                                                      dataFrame['revenue'],
                                                        revenue_difference.alias("revenue_difference")
                                                )"""
def convert_time_2_seconds(t):
    cur = '2017-01-15T14:36:17.583Z'
    #return (datetime.datetime.strptime(cur, '%Y-%m-%dT%H:%M:%S.%fZ') - datetime.datetime.strptime(t, '%Y-%m-%dT%H:%M:%S.%fZ')).total_seconds()/60/60
    return (datetime.datetime.strptime(t, '%Y-%m-%dT%H:%M:%S.%fZ') - datetime.datetime.strptime(cur, '%Y-%m-%dT%H:%M:%S.%fZ')).total_seconds()/60/60
"""
def to_seconds(t)
    return t.total_seconds()
"""

convert_time_udf = udf(convert_time_2_seconds)


def save_usr_times():
    df = spark.read.parquet(os.path.join(TRACE_PARQ_DIR, 'time_method_repoinfo.parquet'))
    df = df.filter(df.manifest != '')
    df.show()

    df = df.withColumn('time', convert_time_udf('timestamp'))
    df.show()

    """
    df = df.withColumn("prev_value", F.lag(df.value).over(my_window))
    df = df.withColumn("diff", F.when(F.isnull(df.value - df.prev_value), 0)
                                  .otherwise(df.value - df.prev_value))
    """

    windowSpec = Window.partitionBy(df.usrname).orderBy(df.time)
    #usr_activetime = convert_time_udf(df.timestamp) - F.first(convert_time_udf(df.timestamp)).over(windowSpec) #convert_time_udf(df.timestamp)).total_seconds()
    df = df.withColumn('prev_time', F.lag(df.time).over(windowSpec))
    #usr_activetime = F.max(df.time).over(windowSpec) - df.time
    df = df.withColumn('time_interval', (df.time - df.prev_time))
    df.show()
    #df.select('usrname', usr_activetime.alias('time_interval'), 'repo_name', 'timestamp').show()
    df = df.select('usrname', 'time_interval', 'repo_name', 'timestamp')
    df.coalesce(400).write.save(os.path.join(TRACE_PARQ_DIR, 'usr_repo_times.parquet'), format='parquet', mode='append')
    export_parquet_2_csv('usr_repo_times.parquet')


# usr = reuse/total ratio
df = spark.read.parquet(os.path.join(TRACE_PARQ_DIR, 'time_method_repoinfo.parquet'))
df = df.filter(df.manifest != '')
df.show()
df = df.withColumn('time', convert_time_udf('timestamp'))
df.show()
windowSpec = Window.partitionBy(df.)


#df = df.partitionby('usrname')
#print("=======>", df.rdd.getNumPartitions())
#print("=======>", spark.read.parquet(os.path.join(TRACE_PARQ_DIR, 'usr_blob_GP_cnt.parquet')).count())

#df = blob_df.groupby('repoinfo.repo_name').agg(F.size(collect_list('method')).where())
#usr2repo2layermap.show()
#spark.read.json(temp_filename, multiLine=True).write.save("/var/1g_json.parquet", format="parquet", mode='append')

#df
#spark.read.json(sc.wholeTextFiles(temp_filename)).write.save("/var/1g_json.parquet", format="parquet")
#sqlContext.jsonFile("data.json")
#string = sc.wholeTextFiles(temp_filename).flatMap(lambda x: ijson.parser(x[1]))
#print(stringlist)
#string = [str(i.value) for i in stringlist]
#df = sqlContext.createDataFrame(string)
#df.write.save("/var/1g_json.parquet", format="parquet")
#rdd = sc.textFile(temp_filename)
#rdd.map(json.loads).cache().toDF().write.save("/var/1g_json.parquet", format="parquet")
#saveAsParquetFile("/var/1g_json.parquet")
#layer_id = df.select("layer_id")
#layer_id.createOrReplaceTempView("layer_id_tables")
#layer_id.cache()
#spark.table("layer_id_tables").write.saveAsTable('/var/layer_table')
#layer_id.write.csv("/var/layer_id.csv")
