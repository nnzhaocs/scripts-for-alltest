
from analysis_library import *


layer_file_cnt = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_file_cnt.parquet')
unique_file_layer_mapping = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'unique_file_layer_mapping.parquet')
unique_cnt_size = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_cnt_size.parquet')
layer_size_file_infos = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_size_file_infos.parquet')
unique_file_info = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_file_info.parquet')
layer_file_dropduplicas = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_file_dropduplicas.parquet')
layer_capacity_dup_ratio = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_capacity_dup_ratio.parquet')


def main():

    sc, spark = init_spark_cluster()
    #save_unique_file_layer_mapping(spark, sc)
    #save_layer_file_cnt(spark, sc)
    #find_file_digest_in_layer(spark, sc)
    save_layer_capacity_redundant_info(spark, sc)
    #save_layer_uniq_shared_size(spark, sc)


def save_layer_capacity_redundant_info(spark, sc):
    #layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
    #size_df = layer_db_df.select('layer_id', 'size.archival_size',
    #                          'size.compressed_size_with_method_gzip',
    #                          'size.uncompressed_sum_of_files')

    df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    #new_df = df.join(size_df, ['layer_id'], 'leftsemi')
    #df.show(20, False)
    fileinfo = spark.read.parquet(unique_file_info)    
    file_df = fileinfo.select(fileinfo.sha256.alias('digest'), 'avg', 'cnt')
    #file_df.show(20, False)
    lf_info = df.join(file_df, ['digest'], 'outer')
    #newer_df.show()
    #newer_df.coalesce(4000).write.save(layer_size_file_infos)


#def save_layer_file_dropduplicas(spark, sc):
    #df = spark.read.parquet(layer_size_file_infos)
    lf_uniq = lf_info.dropDuplicates(['layer_id', 'digest'])
    #lf_uniq.show()
    #new_df = new_df.withColumn('is_uniq_or_not', )
    #new_df.save.write.save(layer_file_dropduplicas)


#def save_layer_uniq_shared_size(spark, sc):
    #df = spark.read.parquet(layer_file_dropduplicas)
    uniq_size = lf_uniq.groupby('layer_id').agg(F.sum('avg').alias('sum_files_dropduplicates'))
    
    #layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
    #size_df = layer_db_df.select('layer_id', layer_db_df.size.archival_size.alias('archival_size'),
    #                          layer_db_df.size.compressed_size_with_method_gzip.alias('gzip_size'),
    #                          layer_db_df.size.uncompressed_sum_of_files.alias('sum_files'))
    #size_df.show()
    size_df = lf_info.groupby('layer_id').agg(F.sum('avg').alias('sum_files'))

    new_df = uniq_size.join(size_df, ['layer_id'], 'outer')

    #shared_df = #spark.read.parquet(layer_size_file_infos)
    shared_df = lf_info.filter('cnt > 1')
    shared_df_layer = shared_df.groupby('layer_id').agg(F.sum('avg').alias('sum_shared_files'))

    newer_df = new_df.join(shared_df_layer, ['layer_id'], 'outer')

    #new = newer_df.withColumn('dropduplicates_ratio', (F.col('sum_files_dropduplicates')/F.col('gzip_size')))
    new = newer_df.withColumn('dropduplicates_ratio_logical', 1-(F.col('sum_files_dropduplicates')/F.col('sum_files')))

    #new = new.withColumn('shared_ratio', (F.col('sum_shared_files')/F.col('gzip_size')))
    new = new.withColumn('shared_ratio_logical', (F.col('sum_shared_files')/F.col('sum_files')))

    new.write.save(layer_capacity_dup_ratio)


def save_layer_file_cnt(spark, sc):
    df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    new_df = df.groupby('layer_id').agg(F.size(F.collect_list('digest')))
    new_df.write.csv(layer_file_cnt)


def get_items(lst):
    if not len(lst):
        return None
    return lst[0]


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
