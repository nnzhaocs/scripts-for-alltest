
# -*- coding: utf-8 -*-
from analysis_library import *

layer_dir_file_mapping = os.path.join(REDUNDANT_DIR_ANALYSIS_DIR, 'layer_dir_file_mapping.parquet')
dir_dup_ratio = os.path.join(REDUNDANT_DIR_ANALYSIS_DIR, 'dir_dup_ratio.parquet')
dirs_basic_info = os.path.join(REDUNDANT_DIR_ANALYSIS_DIR, 'dirs_basic_info.parquet')

def main():

    sc, spark = init_spark_cluster()
    # save_image_info(spark, sc)
    # save_image_layer_mapping(spark, sc)


def save_layer_dir_file_mapping(spark, sc):
    layer_file_df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)

    covert_to_dirname = udf(lambda s: os.path.dirname(s))

    df = layer_file_df.withColumn('dirname', covert_to_dirname('filename').alias('dirname'))

    df.show(20, False)
    df.save.parquet(layer_dir_file_mapping)


def save_dir_info(spark, sc):
    layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
    dirs = layer_db_df.selectExpr("explode(dirs) As structdirs")

    dirsinfo = dirs.select('dir_depth', 'dir_size', 'file_cnt', 'subdir')
    dirsinfo.save.parquet(dirs_basic_info)


def save_dir_dup_ratio_capacity(spark, sc):
    # image_layer_df = spark.read.parquet(image_layer_mapping_shared_pull_cnt).select('image_realname', 'layer_id')
    layer_file_df = spark.read.parquet(layer_dir_file_mapping).select('layer_id', 'dirname', 'digest')
    fileinfo = spark.read.parquet(unique_size_cnt_total_sum)
    file_info_df = fileinfo.select(fileinfo.sha256.alias('digest'), 'avg', 'cnt')

    lf_info = layer_file_df.join(file_info_df, ['digest'], 'inner')

    # image_file_info = image_layer_df.join(lf_info, ['layer_id'], 'inner')
    """get uniq files for image"""
    ld_file_uniq = lf_info.dropDuplicates(['layer_id', 'dirname', 'digest'])

    uniq_size = ld_file_uniq.groupby(['layer_id', 'dirname']).agg(F.sum('avg').alias('sum_files_dropduplicates'),
                                                                  F.size(F.collect_list('avg')).alias('cnt_files_dropduplicates'))

    size_df = lf_info.groupby(['layer_id', 'dirname']).agg(F.sum('avg').alias('sum_files'),
                                                           F.size(F.collect_list('avg')).alias(
                                                               'cnt_files'))

    new_df = uniq_size.join(size_df, ['layer_id', 'dirname'], 'inner')

    shared_df = lf_info.filter('cnt > 1')

    shared_df_layer = shared_df.groupby(['layer_id', 'dirname']).agg(F.sum('avg').alias('sum_shared_files'),
                                                                     F.size(F.collect_list('avg')).alias(
                                                                         'cnt_shared_files'))

    newer_df = new_df.join(shared_df_layer, ['layer_id', 'dirname'], 'inner')

    new = newer_df.withColumn('intra_dir_dup_ratio',
                              (F.col('sum_files') - F.col('sum_files_dropduplicates')) / F.col('sum_files'))

    new = new.withColumn('inter_dir_dup_ratio', (F.col('sum_shared_files') / F.col('sum_files')))

    new = new.withColumn('intra_dir_dup_ratio_cnt',
                         (F.col('cnt_files') - F.col('cnt_files_dropduplicates')) / F.col('cnt_files'))
    new = new.withColumn('inter_dir_dup_ratio_cnt', (F.col('cnt_shared_files') / F.col('cnt_files')))

    new.write.save(dir_dup_ratio)



if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'