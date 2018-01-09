
from analysis_library import *


layer_file_cnt = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_file_cnt.parquet')
unique_file_layer_mapping = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'unique_file_layer_mapping.parquet')
unique_cnt_size = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_cnt_size.parquet')
layer_size_files = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_size_files.parquet')


def main():

    sc, spark = init_spark_cluster()
    #save_unique_file_layer_mapping(spark, sc)
    #save_layer_file_cnt(spark, sc)
    find_file_digest_in_layer(spark, sc)


def save_layer_size(spark, sc):
    layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
    size_df = layer_db_df.select('layer_id', 'size.archival_size',
                              'size.compressed_size_with_method_gzip',
                              'size.uncompressed_sum_of_files')

    df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    new_df = df.join(size_df, ['layer_id'], 'leftsemi')
    new_df.save.write(layer_size_files)


def save_layer_file_size(spark, sc):





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
