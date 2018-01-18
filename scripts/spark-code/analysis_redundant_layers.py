
from analysis_library import *


layer_file_cnt = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_file_cnt.parquet')
unique_file_layer_mapping = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'unique_file_layer_mapping.parquet')
layer_dup_ratio = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_dup_ratio.parquet')


def main():

    sc, spark = init_spark_cluster()
    #save_unique_file_layer_mapping(spark, sc)
    #save_layer_file_cnt(spark, sc)
    #find_file_digest_in_layer(spark, sc)
    #save_layer_redundant_info(spark, sc)
    #save_layer_uniq_shared_size(spark, sc)
    save_layer_info(spark, sc)
    # layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR).dropDuplicates('layer_id')
    # files = layer_db_df.selectExpr("explode(dirs) As structdirs").selectExpr(
    #     "explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
    # regular_files = files.filter(files.sha256.isNotNull())

def save_layer_info(spark, sc):
    layer_db_df = spark.read.parquet(layer_db_absfilename3)#.dropDuplicates('layer_id')
    layerinfo_df = layer_db_df.select('layer_id', layer_db_df.size.archival_size.alias('archival_size'),
                             layer_db_df.size.compressed_size_with_method_gzip.alias('compressed_size'),
                             layer_db_df.size.uncompressed_sum_of_files.alias('uncompressed_size'),
                                 'file_cnt', 'layer_depth.dir_max_depth', F.size('dirs').alias('dir_cnt'))

    layerinfo_df.coalesce(4000).write.save(layer_basic_info3)

# layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR).dropDuplicates('layer_id')
# files = layer_db_df.selectExpr('layer_id', "explode(dirs) As structdirs").selectExpr(
#     'layer_id', 'structdirs.subdir', "explode(structdirs.files) As structdirs_files").selectExpr(
#     'layer_id', 'structdirs.subdir', "structdirs_files.sha256")
# regular_files = files.filter(files.sha256.isNotNull())

def save_layer_redundant_info(spark, sc):

    df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)

    # layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR).dropDuplicates(['layer_id'])
    # files = layer_db_df.selectExpr('layer_id', "explode(dirs) As structdirs").selectExpr(
    #     'layer_id', "explode(structdirs.files) As structdirs_files").selectExpr(
    #     'layer_id', "structdirs_files.sha256")
    # regular_files = files.filter(files.sha256.isNotNull())

    df = df.select('layer_id', 'digest')

    #df.show(20, False)
    fileinfo = spark.read.parquet(unique_size_cnt_total_sum)
    file_df = fileinfo.select(fileinfo.sha256.alias('digest'), 'avg', 'cnt')
    #file_df.show(20, False)
    lf_info = df.join(file_df, ['digest'], 'inner')

    lf_uniq = lf_info.dropDuplicates(['layer_id', 'digest'])

    uniq_size = lf_uniq.groupby('layer_id').agg(F.sum('avg').alias('sum_files_dropduplicates'),
                                                F.size(F.collect_list('avg')).alias(
                                                                  'cnt_files_dropduplicates'))

    size_df = lf_info.groupby('layer_id').agg(F.sum('avg').alias('sum_files'),
                                              F.size(F.collect_list('avg')).alias(
                                                  'cnt_files')
                                              )

    new_df = uniq_size.join(size_df, ['layer_id'], 'inner')

    shared_df = lf_info.filter('cnt > 1')
    shared_df_layer = shared_df.groupby('layer_id').agg(F.sum('avg').alias('sum_shared_files'),
                                                        F.size(F.collect_list('avg')).alias(
                                                            'cnt_shared_files')
                                                        )

    newer_df = new_df.join(shared_df_layer, ['layer_id'], 'inner')

    new = newer_df.withColumn('intra_layer_dup_ratio',
                              (F.col('sum_files') - F.col('sum_files_dropduplicates')) / F.col('sum_files'))

    new = new.withColumn('inter_layer_dup_ratio', (F.col('sum_shared_files') / F.col('sum_files')))

    new = new.withColumn('intra_layer_dup_ratio_cnt',
                         (F.col('cnt_files') - F.col('cnt_files_dropduplicates')) / F.col('cnt_files'))
    new = new.withColumn('inter_layer_dup_ratio_cnt', (F.col('cnt_shared_files') / F.col('cnt_files')))

    # new = newer_df.withColumn('dropduplicates_ratio_logical', 1-(F.col('sum_files_dropduplicates')/F.col('sum_files')))
    # new = new.withColumn('shared_ratio_logical', (F.col('sum_shared_files')/F.col('sum_files')))
    new.write.save(layer_dup_ratio)


def save_layer_file_cnt(spark, sc):
    df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    new_df = df.groupby('layer_id').agg(F.size(F.collect_list('digest')))
    new_df.write.csv(layer_file_cnt)


def find_file_digest_in_layer(spark, sc):
    df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    df.filter(df.filename == '/usr/portage/x11-libs/xcb-util-cursor/Manifest').show()


"""save uniqe file digests"""
def save_unique_file_layer_mapping(spark, sc):
    df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    new_df = df.dropDuplicates(['digest'])
    #df.show()
    # func = F.udf(get_items, StringType())
    # new_df = df.groupby('digest').agg(func(F.collect_list('filename')).alias('filename'), func(F.collect_list('layer_id')).alias('layer_id'))
    new_df = new_df.select('layer_id','filename')
    new_df.write.csv(unique_file_layer_mapping)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
