# -*- coding: utf-8 -*-
#import sys
#sys.path.append('../plotter/')
#from draw_pic import *
from analysis_library import *
# import pandas as pd


draw_type_by_repeat_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_type_by_repeat_cnt.csv')
draw_type_by_total_sum = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_type_by_total_sum.csv')
draw_type1_by_repeat_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_type1_by_repeat_cnt.csv')
draw_type_by_size = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_type_by_size.csv')
draw_type_by_dup_ratio_cap = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_type_by_dup_ratio_cap.csv')
draw_type_by_dup_ratio_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_type_by_dup_ratio_cnt.csv')
capacity_data = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'capacity_data.csv')
draw_size_uniq = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_size_uniq.csv')
draw_size_shared = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_size_shared.csv')
draw_size_whole = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_size_whole.csv')
draw_repeat_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_repeat_cnt.csv')


def main():
    sc, spark = init_spark_cluster()
    save_file_type_by_repeat_cnt(spark, sc)
    #save_file_size(spark, sc)
    #save_file_repeat_cnt(spark, sc)
    #calculate_capacity(spark, sc)


def save_file_type_by_repeat_cnt(spark, sc):
    """save by repeat cnt"""
    file_info = spark.read.parquet(unique_file_basic_info).filter(F.size('type') > 0)

    func = F.udf(lambda s: s[0].split(" ")[0])
    func0 = F.udf(lambda s: s[0] if len(s) else None)

    file_info_df = file_info.select(func0('file_name').alias('filename'), 'cnt', func('type').alias('type'), func0('extension').alias('extension'),
                'total_sum', 'sha256', 'avg')
    file_info_df = file_info_df.filter(file_info_df.type.isNotNull())
    file_info_df.printSchema()
    """
    sort_cnt = file_info_df.sort(file_info_df.cnt.desc())
    sort_cnt.show()
    sort_cnt.write.csv(draw_type_by_repeat_cnt)

    sort_cap = file_info_df.sort(file_info_df.total_sum.desc())
    sort_cap.show()
    sort_cap.write.csv(draw_type_by_total_sum)
    """
    
    type1 = file_info_df.groupby('type').agg(F.sum('cnt').alias('total_cnt'), F.sum('avg').alias('size_dropdup'),
                                            F.sum('total_sum').alias('sum_size'),
                                            func0(F.collect_set('extension')).alias('extension'),
                                            F.size(F.collect_list('cnt')).alias('uniq_cnt'))
    type1.printSchema()
    
    type1 = type1.withColumn('avg_size', F.col('sum_size')*1.0/F.col('total_cnt'))
    type1 = type1.withColumn('dup_ratio_cap', (F.col('sum_size') - F.col('size_dropdup'))/F.col('sum_size')*1.0)
    type1 = type1.withColumn('dup_ratio_cnt', (F.col('total_cnt') - F.col('uniq_cnt'))* 1.0 / F.col('total_cnt'))

    sort_type_cnt = type1.sort(type1.total_cnt.desc())
    #sort_type_cnt.show()
    sort_type_cnt.write.csv(draw_type1_by_repeat_cnt)

    sort_type_size = type1.sort(type1.sum_size.desc())
    #sort_type_size.show()
    sort_type_size.write.csv(draw_type_by_size)

    sort_type_dup_r = type1.sort(type1.dup_ratio_cap.desc())
    #sort_type_dup_r.show()
    sort_type_dup_r.write.csv(draw_type_by_dup_ratio_cap)

    sort_type_dup_r = type1.sort(type1.dup_ratio_cnt.desc())
    #sort_type_dup_r.show()
    sort_type_dup_r.write.csv(draw_type_by_dup_ratio_cnt)
    

def calculate_capacity(spark, sc):
    df = spark.read.parquet(unique_size_cnt_total_sum)
    """
    df_1 = df.filter(df.cnt == 1)
    df_1_size = df_1.select(F.sum('avg').alias('sum_size_1'))
    df_1_size.show()
    print("==============> df_1:%d", df_1.count())
    #df_1_size.show()
    """
    
    df_2 = df.filter(df.cnt > 1)
    """#df_2_dedup.show()
    df_2_dedup_size = df_2.select(F.sum('avg').alias('sum_size_2_remove_redundancy'))
    df_2_dedup_size.show()
    print("==============> df_2:%d", df_2.count())
    """
    #new_df = df_2.unionAll(df_1)
    """
    #df_2_total_size = df.filter(df.cnt > 1)
    #df_2.show()
    df_2_with_dup = df_2.select(F.sum('total_sum').alias('sum_size_2_redundancy'))
    df_2_with_dup_cnt = df_2.select(F.sum('cnt').alias('total_cnt_2'))
    df_2_with_dup.show()
    df_2_with_dup_cnt.show()
    """
    #new_df = new_df.unionAll(df_2)
    
    df_2_dedup_size_total = df.select(F.sum('avg').alias('sum_files_remove_redundancy'))
    df_2_dedup_size_total.show(20, False)
    print("==============> dedup_cnt_total:%d", df.count())
    #df_1.show()
    #new_df = new_df.unionAll(df_1)
    """
    df_dup_size_total = df.select(F.sum('total_sum').alias('sum_files_redundancy'))
    df_dup_size_total.show()
    df_with_dup_cnt_total = df.select(F.sum('cnt').alias('total_cnt'))
    df_with_dup_cnt_total.show()
    """
    #print("=============> ")
    #df.show()
    #df_dup_size_total.coalesce(1).write.csv(capacity_data, mode='append')
    #df_2_dedup_size_total.coalesce(1).write.csv(capacity_data, mode='append')
    #df_2_with_dup.coalesce(1).write.csv(capacity_data, mode='append')
    #df_2_dedup_size.coalesce(1).write.csv(capacity_data, mode='append')
    #df_1_size.coalesce(1).write.csv(capacity_data, mode='append')
    #new_df = df_dup_size_total.unionAll(df_2_dedup_size_total, df_2_with_dup, df_2_dedup_size, df_1_size) 
    #new_df.show(20, False)
    #new_df.write.csv(capacity_data)
    #print("==============> df_dup_size_total=%d, df_2_dedup_size_total=%d, df_2_with_dup=%d, df_2_dedup_size=%d, df_1_size=%d", df.count(), df_2_with_dup.count(), df_2_dedup_size.count(), df_1_size.count())
    

def save_file_size(spark, sc):
    size_df = spark.read.parquet(unique_size_cnt_total_sum)#.select((F.col('avg')/1024.0).alias('avg')) #kb
    size_df.show()
    size_uniq = size_df.filter(size_df.cnt == 1).select('avg')
    size_shared = size_df.filter(size_df.cnt > 1).select('avg')
    size_whole = size_df.select('avg')

    size_uniq = size_uniq.sort('avg')
    size_shared = size_shared.sort('avg')
    size_whole = size_whole.sort('avg')

    size_uniq.coalesce(1).write.csv(draw_size_uniq)
    size_shared.coalesce(1).write.csv(draw_size_shared)
    size_whole.coalesce(1).write.csv(draw_size_whole)


def save_file_repeat_cnt(spark, sc):
    cnt_df  = spark.read.parquet(unique_size_cnt_total_sum).select('cnt')
    cnt = cnt_df.sort(cnt_df.cnt.desc())
    cnt.write.csv(draw_repeat_cnt)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
