
# -*- coding: utf-8 -*-
import os, json, logging
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

HDFS_DIR = 'hdfs://hulk0:8020/'
MANIFESTS_DIR = os.path.join(HDFS_DIR, "2tb_hdd/manifests")
VAR_DIR = os.path.join(HDFS_DIR, 'var')

output_absfilename = os.path.join(VAR_DIR, 'manifests.parquet')

""".set("spark.executor.cores", 5) \
    .set("spark.driver.memory", "10g") \
    .set("spark.executor.memory", "40g") \ 
"""

EXECUTOR_CORES = 5
EXECUTOR_MEMORY = "40g"
DRIVER_MEMORY = "10g"


def manifest_schemalist(manifest):
    blobs_digest = []
    if 'manifests' in manifest and isinstance(manifest['manifests'], list) and len(manifest['manifests']) > 0:
        for i in manifest['manifests']:
            if 'digest' in i:
                #print i['digest']
                blobs_digest.append(i['digest'])
    return blobs_digest


def manifest_schema2(manifest, r_type):
    blobs_digest = []
    if r_type == 'config':
        if 'config' in manifest and 'digest' in manifest['config']:
            config_digest = manifest['config']['digest']
            blobs_digest.append(config_digest)
            return blobs_digest
    elif r_type == 'layers':
        if 'layers' in manifest and isinstance(manifest['layers'], list) and len(manifest['layers']) > 0:
            for i in manifest['layers']:
                if 'digest' in i:
                    #print i['digest']
                    blobs_digest.append(i['digest'])
            return blobs_digest
    else:
        print "################## which one to load? config or layers ##################"


def manifest_schema1(manifest):
    blobs_digest = []
    if 'fsLayers' in manifest and isinstance(manifest['fsLayers'], list) and len(manifest['fsLayers']) > 0:
        for i in manifest['fsLayers']:
            if 'blobSum' in i:
                #print i['blobSum']
                blobs_digest.append(i['blobSum'])
    return list(set(blobs_digest)) # blobs_digest


def parse_manifest(pair):

    manifest_absfilename, file_content = pair
    manifest_json_data = json.loads(file_content)

    blobs_digest = []
    config_digest = []
    version = ""

    logging.debug("===================> process manifest_filename: %s" % manifest_absfilename)
    manifest_filename = os.path.basename(manifest_absfilename)

    if 'schemaVersion' in manifest_json_data and manifest_json_data['schemaVersion'] == 2:
        if 'mediaType' in manifest_json_data and 'list' in manifest_json_data['mediaType']:
            blobs_digest = manifest_schemalist(manifest_json_data)
            version = 'schema2.list'
        else:
            config_digest = manifest_schema2(manifest_json_data, 'config')
            blobs_digest = manifest_schema2(manifest_json_data, 'layers')
            version = 'schema2'
            # print('config_digest:%s; blobs_digests: %s; version: %s'% (config_digest, blobs_digest, version))
    elif 'schemaVersion' in manifest_json_data and manifest_json_data['schemaVersion'] == 1:
        blobs_digest = manifest_schema1(manifest_json_data)
        version = 'schema1'

    image_manifest = {
        'version': version,
        'manifest': manifest_filename,
        'config': config_digest,
        'layers': blobs_digest
    }

    return image_manifest


def parse_and_store_manifest(input_dirname, output_absfilename, sc, spark):

    json_data_rdd = sc.wholeTextFiles(input_dirname).flatMap(parse_manifest)
    json_data_rdd.toDF().write.save(output_absfilename, format="parquet", mode='append')


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
    fmt = "%(funcName)s():%(lineno)i: %(message)s %(levelname)s"
    logging.basicConfig(level=logging.DEBUG, format=fmt)

    sc, spark = init_spark_cluster()

    parse_and_store_manifest(MANIFESTS_DIR, output_absfilename, sc, spark)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'