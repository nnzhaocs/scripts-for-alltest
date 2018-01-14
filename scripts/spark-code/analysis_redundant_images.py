
# -*- coding: utf-8 -*-
from analysis_library import *


pull_cnt_absfilename = os.path.join(LOCAL_DIR, "image_pull_cnt.txt")
manifest_analyze_or_not = os.path.join(VAR_DIR, 'manifests_analyzed_or_not.parquet')
image_info = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_info.parquet')
image_layer_mapping_shared_pull_cnt = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_layer_mapping_shared_pull_cnt.parquet')
image_dup_ratio = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_dup_ratio.parquet')


def main():

    sc, spark = init_spark_cluster()
    save_image_info(spark, sc)
    # save_image_layer_mapping(spark, sc)


def save_image_info(spark, sc):
    df = spark.read.parquet(manifest_analyze_or_not)
    df = df.filter(df.layer_id_analyzed_or_not == True).select('filename', 'schemaVersion', 'layer_id')

    conver_imagenames = F.udf(lambda s: s.split('-latest')[0] + '-latest')

    manifest_df = df.withColumn('imagename', conver_imagenames('filename'))  # .show(20, False)

    manifest_df = manifest_df.dropDuplicates(['imagename'])
    print("manifest_df =======> %d", manifest_df.count())

    df_t = spark.read.format("csv").load(pull_cnt_absfilename).select(df._c1.alias('pull_cnt'), df._c2.alias('image_realname'))

    convert_imagenames = udf(lambda s: "library-"+ s +'-latest' if '/' not in s else s.replace("/", "-") + '-latest')

    pull_cnt_df = df_t.withColumn('imagename', convert_imagenames(df.image_realname))

    pull_cnt_df = pull_cnt_df.dropDuplicates(['imagename'])
    print("pull_cnt_df =======> %d", pull_cnt_df.count())

    new_df = pull_cnt_df.join(manifest_df, ['imagename'], 'inner')

    new_df.show(20, False)
    print("new_df =======> %d", new_df.count())

    new_df.write.parquet(image_info)


def save_image_basic_info(spark, sc):
    df = spark.read.parquet(image_info).select('image_realname', 'layer_id')
    image_layer = df.select('image_realname', F.explode('layer_id'))
    image_layer = image_layer.dropDuplicates(['image_realname', 'layer_id'])

    layer_basic_info_df = spark.read.parquet(layer_basic_info)
    layerinfo_df = layer_basic_info_df.select('layer_id', 'archival_size',
                             'compressed_size', 'uncompressed_size',
                                 'file_cnt', 'dir_cnt')

    image_basic_info = image_layer.join(layerinfo_df, ['layer_id'], 'inner')

    image_basic_info = image_basic_info.groupby('image_realname').agg(F.sum('archival_size').alias('archival_size'),
                                                F.sum('compressed_size').alias('compressed_size'),
                                                F.sum('uncompressed_size').alias('uncompressed_size'),
                                                F.sum('file_cnt').alias('file_cnt'),
                                                F.sum('dir_cnt').alias('dir_cnt'))

    layerinfo_df.save.parquet(image_basic_info)


def save_image_layer_mapping(spark, sc):
    df = spark.read.parquet(image_info).select('image_realname', 'layer_id')
    image_layer = df.select('image_realname', F.explode('layer_id'))

    image_layer = image_layer.dropDuplicates(['image_realname', 'layer_id'])

    pull_cnt = spark.read.parquet(image_info).select('image_realname', 'pull_cnt')

    image_layer_pull = image_layer.join(pull_cnt, ['image_realname'], 'inner')

    layer_pull_cnt = image_layer_pull.groupby('layer_id').agg(F.sum('pull_cnt').alias('layer_pull_cnt'))

    shared_layer = image_layer.groupby('layer_id').agg(F.size(F.collect_set('image_realname')).alias('shared_image_cnt'))

    testlayer = udf(lambda n: 1 if n > 1 else 0)

    shared_layer = shared_layer.withColumn('shared_or_not', testlayer('shared_image_cnt'))

    layer = layer_pull_cnt.join(shared_layer, 'layer_id', 'inner')

    image_layer_mapping = image_layer.join(layer, 'layer_id', 'inner')

    image_layer_mapping.save.parquet(image_layer_mapping_shared_pull_cnt)


def save_image_dup_ratio_capacity(spark, sc):
    image_layer_df = spark.read.parquet(image_layer_mapping_shared_pull_cnt).select('image_realname', 'layer_id')
    layer_file_df = spark.read.parquet(LAYER_FILE_MAPPING_DIR).select('layer_id', 'digest')
    fileinfo = spark.read.parquet(unique_size_cnt_total_sum)
    file_info_df = fileinfo.select(fileinfo.sha256.alias('digest'), 'avg', 'cnt')

    lf_info = layer_file_df.join(file_info_df, ['digest'], 'inner')

    image_file_info = image_layer_df.join(lf_info, ['layer_id'], 'inner')
    """get uniq files for image"""
    image_file_uniq = lf_info.dropDuplicates(['image_realname', 'digest'])

    uniq_size = image_file_uniq.groupby('image_realname').agg(F.sum('avg').alias('sum_files_dropduplicates'),
                                                              F.size(F.collect_list('avg')).alias(
                                                                  'cnt_files_dropduplicates'))

    size_df = image_file_info.groupby('image_realname').agg(F.sum('avg').alias('sum_files'),
                                                            F.size(F.collect_list('avg')).alias(
                                                               'cnt_files'))

    new_df = uniq_size.join(size_df, ['image_realname'], 'inner')

    shared_df = image_file_info.filter('cnt > 1')

    shared_df_image = shared_df.groupby('image_realname').agg(F.sum('avg').alias('sum_shared_files'),
                                                              F.size(F.collect_list('avg')).alias(
                                                                         'cnt_shared_files'))

    newer_df = new_df.join(shared_df_image, ['image_realname'], 'inner')

    new = newer_df.withColumn('intra_image_dup_ratio',
                              (F.col('sum_files') - F.col('sum_files_dropduplicates')) / F.col('sum_files'))

    new = new.withColumn('inter_image_dup_ratio', (F.col('sum_shared_files') / F.col('sum_files')))

    new = new.withColumn('intra_image_dup_ratio_cnt',
                         (F.col('cnt_files') - F.col('cnt_files_dropduplicates')) / F.col('cnt_files'))
    new = new.withColumn('inter_image_dup_ratio_cnt', (F.col('cnt_shared_files') / F.col('cnt_files')))

    new.write.save(image_dup_ratio)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'