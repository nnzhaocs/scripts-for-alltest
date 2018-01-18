# -*- coding: utf-8 -*-
from analysis_library import *


# pull_cnt_absfilename = os.path.join(LOCAL_DIR, "image_pull_cnt.txt")
# manifest_analyze_or_not = os.path.join(MANIFESTS_DIR, 'manifests_analyzed_or_not.parquet')
image_info = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_info.parquet')
image_layer_mapping_shared_pull_cnt = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_layer_mapping_shared_pull_cnt.parquet')
image_dup_ratio = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_dup_ratio.parquet')

draw_image_pull_cnt = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'draw_image_pull_cnt')
draw_image_layer_cnt = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'draw_image_layer_cnt')
draw_image_basic_info = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'draw_image_basic_info')
draw_layer_pull_cnt = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'draw_layer_pull_cnt')
draw_layer_shared_cnt = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'draw_layer_shared_cnt')
draw_image_layer_shared_cnt = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'draw_image_layer_shared_cnt')
draw_image_dup_ratio = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'draw_image_dup_ratio')
draw_layer_basic_info = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'draw_layer_basic_info')


def main():
    sc, spark = init_spark_cluster()
    #save_file_type_by_repeat_cnt(spark, sc)
    #save_file_size(spark, sc)
    #save_file_repeat_cnt(spark, sc)
    # calculate_capacity(spark, sc)


def save_image_info(spark, sc):
    info = spark.read.parquet(image_info)
    info.write.csv(draw_image_pull_cnt)
    info.withColumn('layer_cnt',
                    F.size('layer_id')).select('layer_cnt').write.csv(draw_image_layer_cnt)

    image_info_data = spark.read.parquet(image_basic_info)
    image_info_data.select('archival_size', 'compressed_size', 'uncompressed_size', 'file_cnt',
                            'dir_cnt').write.csv(draw_image_basic_info)

    image_layer_mapping = spark.read.parquet(image_layer_mapping_shared_pull_cnt)
    image_layer_mapping.select('layer_pull_cnt').save.csv(draw_layer_pull_cnt)

    image_layer_mapping.select('shared_image_cnt').save.csv(draw_layer_shared_cnt)

    fd = image_layer_mapping.groupby('image_realname').agg(F.sum('shared_or_not').alias('shared_cnt'),
                                                      F.size(F.collect_list('shared_or_not')).alias('layer_cnt'))
    fd.withColumn('shared_layer_ratio',
                  F.col('shared_cnt')/F.col('layer_cnt')*1.0).select('shared_layer_ratio').save.csv(draw_image_layer_shared_cnt)

    spark.read.parquet(image_dup_ratio).select('intra_image_dup_ratio', 'inter_image_dup_ratio',
                                               'intra_image_dup_ratio_cnt', 'inter_image_dup_ratio_cnt').save.csv(draw_image_dup_ratio)

    spark.read.parquet(layer_basic_info1, layer_basic_info2, layer_basic_info3).select(
        'archival_size', 'compressed_size', 'uncompressed_size',
        'file_cnt', '`layer_depth.dir_max_depth`', 'dir_cnt').save.csv(draw_layer_basic_info)



if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'