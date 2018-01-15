
import sys
sys.path.append('../plotter/')
from draw_pic import *
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
    save_file_size(spark, sc)
    save_file_repeat_cnt(spark, sc)
    # calculate_capacity(spark, sc)


def save_file_type_by_repeat_cnt(spark, sc):
    """save by repeat cnt"""
    file_info = spark.read.parquet(unique_file_basic_info).filter(F.size('type') > 0)

    func = F.udf(lambda s: s[0].split(" ")[0])
    func0 = F.udf(lambda s: s[0] if len(s) else None)

    file_info_df = file_info.select(func0('file_name'), 'cnt', func('type').alias('type'), func0('extension').alias('extension'),
                'total_sum', 'sha256', 'avg')
    
    sort_cnt = file_info_df.sort(file_info_df.cnt.desc())
    sort_cnt.show()
    sort_cnt.write.csv(draw_type_by_repeat_cnt)

    sort_cap = file_info_df.sort(file_info_df.total_sum.desc())
    sort_cap.show()
    sort_cap.write.csv(draw_type_by_total_sum)

    type = file_info_df.groupby('type').agg(F.sum('cnt').alias('total_cnt'), F.sum('avg').alias('size_dropdup'),
                                            F.sum('total_sum').alias('sum_size'),
                                            F.collect_set('extension'),
                                            F.size(F.collect_list('cnt')).alias('uniq_cnt'))
    type = type.withColumn('avg_size', F.col('sum_size')*1.0/F.col('cnt'))
    type = type.withColumn('dup_ratio_cap', F.col('size_dropdup')*1.0/F.sum('sum_size'))
    type = type.withColumn('dup_ratio_cnt', F.col('uniq_cnt') * 1.0 / F.sum('total_cnt'))

    sort_type_cnt = type.sort(type.cnt.desc())
    sort_type_cnt.show()
    sort_type_cnt.write.csv(draw_type1_by_repeat_cnt)

    sort_type_size = type.sort(type.sum_size.desc())
    sort_type_size.show()
    sort_type_size.write.csv(draw_type_by_size)

    sort_type_dup_r = type.sort(type.dup_ratio_cap.desc())
    sort_type_dup_r.show()
    sort_type_dup_r.write.csv(draw_type_by_dup_ratio_cap)

    sort_type_dup_r = type.sort(type.dup_ratio_cnt.desc())
    sort_type_dup_r.show()
    sort_type_dup_r.write.csv(draw_type_by_dup_ratio_cnt)


def calculate_capacity(spark, sc):
    df = spark.read.parquet(unique_size_cnt_total_sum)
    df_1 = df.filter(df.cnt == 1)
    df_1.show()
    df_1 = df_1.select(F.sum('avg').alias('sum_size_1'))
    df_1.show()
    
    df_2 = df.filter(df.cnt > 1)
    df_2.show()
    df_2 = df_2.select(F.sum('avg').alias('sum_size_2_remove_redundancy'))
    df_2.show()

    new_df = df_2.unionAll(df_1)

    df_2 = df.filter(df.cnt > 1)
    df_2.show()
    df_2 = df_2.select(F.sum('total_sum').alias('sum_size_2_redundancy'))
    df_2.show()

    new_df = new_df.unionAll(df_2)

    df_1 = df.select(F.sum('avg').alias('sum_files_remove_redundancy'))
    df_1.show()

    new_df = new_df.unionAll(df_1)

    df = df.select(F.sum('total_sum').alias('sum_files_redundancy'))
    df.show()

    new_df = new_df.unionAll(df) 
    new_df.show()
    new_df.write.csv(capacity_data)


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
