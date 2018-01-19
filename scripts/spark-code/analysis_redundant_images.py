
# -*- coding: utf-8 -*-
from analysis_library import *


pull_cnt_absfilename = os.path.join(LOCAL_DIR, "image_pull_cnt.txt")
manifest_analyze_or_not = os.path.join(MANIFESTS_DIR, 'manifests_analyzed_or_not.parquet')
image_info = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_info.parquet')
image_layer_mapping_shared_pull_cnt = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_layer_mapping_shared_pull_cnt.parquet')
image_dup_ratio = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_dup_ratio.parquet')
shared_layer_dup_ratio = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'shared_layer_dup_ratio')
private_layer_dup_ratio = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'private_layer_dup_ratio')


def main():

    sc, spark = init_spark_cluster()
    #save_image_info(spark, sc)
    #save_image_basic_info(spark, sc)
    #save_image_layer_mapping(spark, sc)
    save_image_dup_ratio_capacity(spark, sc)


def save_image_info(spark, sc):
    
    df = spark.read.parquet(manifest_analyze_or_not)
    #df.show(20, False)
    
    df = df.filter(df.layer_id_analyzed_or_not == True).select('filename', 'schemaVersion', 'layer_id')

    conver_filenames = F.udf(lambda s: s.split('-latest')[0] + '-latest')

    manifest_df = df.withColumn('imagename', conver_filenames('filename'))  # .show(20, False)
    #manifest_df.filter(manifest_df.imagename == '0830-docker-whale-latest').show()
    
    manifest_df = manifest_df.dropDuplicates(['imagename'])
    manifest_df.show(2, False)
    print("manifest_df =======> %d", manifest_df.count())
    
    df = spark.read.csv(pull_cnt_absfilename)
    
    df_t = df.select(df._c1.alias('pull_cnt'), df._c2.alias('image_realname'))

    convert_imagenames = F.udf(lambda s: "library-"+ s.replace(' ', '') +'-latest' if '/' not in s else s.replace("/", "-").replace(' ', '') + '-latest')

    pull_cnt_df = df_t.withColumn('imagename', convert_imagenames(df_t.image_realname))

    pull_cnt_df = pull_cnt_df.dropDuplicates(['imagename'])
    #pull_cnt_df.filter(pull_cnt_df.imagename.like('11tracer-docker-whale-latest')).show(20, False)
    pull_cnt_df.show(10, False)
    print("pull_cnt_df =======> %d", pull_cnt_df.count())
    
    new_df = pull_cnt_df.join(manifest_df, ['imagename'], 'inner')

    new_df.show(20, False)
    print("new_df =======> %d", new_df.count())

    new_df.write.parquet(image_info)
    
    
def save_image_basic_info(spark, sc):
    df = spark.read.parquet(image_info).select('image_realname', 'layer_id')
    #df.printSchema() 
    image_layer = df.select('image_realname', F.explode('layer_id').alias('layer_id'))
    #image_layer.show()
    
    image_layer = image_layer.dropDuplicates(['image_realname', 'layer_id'])

    layer_basic_info_df = spark.read.parquet(layer_basic_info1, layer_basic_info2, layer_basic_info3)
    layerinfo_df = layer_basic_info_df.select('layer_id', 'archival_size',
                             'compressed_size', 'uncompressed_size',
                                 'file_cnt', 'dir_cnt')

    image_info_data = image_layer.join(layerinfo_df, ['layer_id'], 'inner')

    image_info_data = image_info_data.groupby('image_realname').agg(F.sum('archival_size').alias('archival_size'),
                                                F.sum('compressed_size').alias('compressed_size'),
                                                F.sum('uncompressed_size').alias('uncompressed_size'),
                                                F.sum('file_cnt').alias('file_cnt'),
                                                F.sum('dir_cnt').alias('dir_cnt'))
    #image_basic_info.show(20, False)
    image_info_data.write.parquet(image_basic_info)
    

def save_image_layer_mapping(spark, sc):
    df = spark.read.parquet(image_info).select('image_realname', 'layer_id')
    image_layer = df.select('image_realname', F.explode('layer_id').alias('layer_id'))

    image_layer = image_layer.dropDuplicates(['image_realname', 'layer_id'])

    pull_cnt = spark.read.parquet(image_info).select('image_realname', 'pull_cnt')

    image_layer_pull = image_layer.join(pull_cnt, ['image_realname'], 'inner')

    layer_pull_cnt = image_layer_pull.groupby('layer_id').agg(F.sum('pull_cnt').alias('layer_pull_cnt'))

    shared_layer = image_layer.groupby('layer_id').agg(F.size(F.collect_set('image_realname')).alias('shared_image_cnt'))

    testlayer = F.udf(lambda n: 1 if n > 1 else 0)

    shared_layer = shared_layer.withColumn('shared_or_not', testlayer('shared_image_cnt'))

    layer = layer_pull_cnt.join(shared_layer, 'layer_id', 'inner')

    image_layer_mapping = image_layer.join(layer, 'layer_id', 'inner')

    #image_layer_mapping.show(20, False)

    image_layer_mapping.write.parquet(image_layer_mapping_shared_pull_cnt)


def save_layers_shared_private_dup_ratio(spark, sc):
    share_or_not = spark.read.parquet(image_layer_mapping_shared_pull_cnt).select('layer_id', 'shared_image_cnt')

    df = spark.read.parquet(LAYER_FILE_MAPPING_DIR).select('layer_id', 'digest')

    fileinfo = spark.read.parquet(unique_size_cnt_total_sum)
    file_df = fileinfo.select(fileinfo.sha256.alias('digest'), 'avg', 'cnt')
    lf_info = df.join(file_df, ['digest'], 'inner')

    lf_shared_or_not = lf_info.join(share_or_not, ['layer_id'], 'inner')

    # ==================> get shared layers dup ratio

    shared_lf_info = lf_shared_or_not.filter('shared_image_cnt > 1')
    lf_info = shared_lf_info

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

    new.write.save(shared_layer_dup_ratio)

    # ========================> get private layer dup ratio
    """
    private_lf_info = lf_shared_or_not.filter('shared_image_cnt == 1')
    lf_info = private_lf_info

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

    new.write.save(private_layer_dup_ratio)
    """


def save_image_dup_ratio_capacity(spark, sc):
    """
    image_layer_df = spark.read.parquet(image_layer_mapping_shared_pull_cnt).select('image_realname', 'layer_id')
    layer_file_df = spark.read.parquet(LAYER_FILE_MAPPING_DIR).select('layer_id', 'digest')
    fileinfo = spark.read.parquet(unique_size_cnt_total_sum)
    file_info_df = fileinfo.select(fileinfo.sha256.alias('digest'), 'avg', 'cnt')

    lf_info = layer_file_df.join(file_info_df, ['digest'], 'inner')

    image_file_info = image_layer_df.join(lf_info, ['layer_id'], 'inner')
    image_file_info = image_file_info.select('image_realname','digest', 'avg', 'cnt')
    image_file_info.write.save('/redundant_image_analysis/image_file_mapping.parquet')
    """
    """get uniq files for image"""
    
    image_file_info = spark.read.parquet('/redundant_image_analysis/image_file_mapping.parquet')   
 
    image_file_uniq = image_file_info.dropDuplicates(['image_realname', 'digest'])
  
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

    #new.show(20, False)

    new.write.save(image_dup_ratio)
    

if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
