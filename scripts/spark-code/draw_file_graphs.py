
import sys
sys.path.append('../plotter/')
from draw_pic import *
from analysis_library import *
import pandas as pd

unique_size_cnt_total_sum = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_size_cnt_total_sum.parquet')
#unique_draw_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_draw_cnt.csv')

RESULTS_DIR = '/home/nannan/4tb_results'

capacity_data = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'capacity_data.csv')

unique_draw_cnt_data = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_draw_cnt_data.csv')
unique_draw_size = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_draw_size.csv')
draw_sum_file_size = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_sum_file_size.csv')
avg_size_by_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'avg_size_by_cnt.parquet')
draw_avg_size_by_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_avg_size_by_cnt')
draw_sum_size_by_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_sum_size_by_cnt')
unique_file_info = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_file_info.parquet')
fileinfo_by_repeat_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'fileinfo_by_repeat_cnt.csv')
fileinfo_by_total_sum = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'fileinfo_by_total_sum.csv')
file_ageinfo = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'file_ageinfo.csv')
age_diffinfo = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'age_diffinfo.csv')

#unique_draw_cnt_data = os.path.join(RESULTS_DIR, 'unique_draw_cnt_data.csv')
#unique_draw_size_data = os.path.join(RESULTS_DIR, 'unique_draw_size_data.csv')
#draw_sum_file_size_data = os.path.join(RESULTS_DIR, 'draw_sum_file_size_data.csv')
#draw_avg_size_by_cnt = os.path.join(RESULTS_DIR, 'draw_avg_size_by_cnt.csv')
#draw_sum_size_by_cnt = os.path.join(RESULTS_DIR, 'draw_sum_size_by_cnt.csv')

cur_time = 1514873764.870036


def main():
    sc, spark = init_spark_cluster()
    #save_file_repeat_cnt(spark, sc)
    #save_file_size(spark, sc)
    #save_sum_file_size(spark, sc)
    #save_avg_size_by_repeat_cnt(spark, sc)
    #save_fileinfo_by_repeat_cnt(spark, sc)
    save_file_age(spark, sc)
    #draw_file_repeat_cnt()
    #draw_file_size()
    #draw_sum_file_size()
    #calculate_capacity(spark, sc)


# def get_items(lst):
#     if not len(lst):
# 	return None
#     return lst[0]

# def get_difference(lst):
#     return (max(lst)-min(lst))
#
#
# def get_mean(lst):
#     return sum(lst)/len(lst)


# def save_file_age(spark, sc):
#     df_f = spark.read.parquet(unique_file_info)
#     func = F.udf(get_mean, FloatType())
#
#     df = df_f.select('cnt', ((cur_time - func('st_ctime'))/60/60/24).alias('st_ctime'), 'total_sum', 'avg') #days
#     df.write.csv(file_ageinfo)
#
#     func = F.udf(get_difference, IntegerType())
#     new_df = df_f.select('cnt', ((cur_time - func('st_ctime'))/60/60/24).alias('st_ctime'), 'total_sum', 'avg') #days
#     new_df.write.csv(age_diffinfo)

def save_file_type_by_repeat_cnt(spark, sc):
    """save by repeat cnt"""
    file_info = spark.read.parquet(unique_file_info)
    # func = F.udf(get_items, StringType())
    # df = df_f.select('cnt', func('file_name').alias('file_name'), func('type').alias('type'), 'avg', 'total_sum')

    func = udf(lambda s: s.split(" ")[0])
    file_info_df = file_info.select('file_name', 'cnt', func('type').alias('type'), 'extension', 'total_sum', 'sha256', 'avg')

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
    sort_type_cnt.write.csv(draw_type_by_repeat_cnt)

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


# def save_avg_size_by_repeat_cnt(spark, sc):
#     df = spark.read.parquet(unique_size_cnt_total_sum)
#     size_cnt = df.groupby('cnt').agg(F.sum('avg').alias('sum_size'), F.size(collect_list('avg')).alias('size_cnt'))
#     size_cnt = size_cnt.withColumn('avg_size_by_cnt', (F.col('sum_size')/F.col('size_cnt')/1024.0)) #kb
#     size_cnt = size_cnt.sort(size_cnt.cnt)
#     size_cnt.write.save(avg_size_by_cnt)
#     df = size_cnt.select('cnt', 'avg_size_by_cnt')
#     df.coalesce(1).write.csv(draw_avg_size_by_cnt)
#     df = size_cnt.select('cnt', (F.col('sum_size')/1024.0/1024.0).alias('sum_size')) #mb
#     df.coalesce(1).write.csv(draw_sum_size_by_cnt)

# def save_sum_file_size(spark, sc):
#     df_f = spark.read.parquet(unique_size_cnt_total_sum).select((F.col('total_sum')/1024.0/1024.0).alias('total_sum')) #mb
#     df = df_f.sort(df_f.total_sum)
#     df.write.csv(draw_sum_file_size)
#
#
def save_file_repeat_cnt(spark, sc):
    cnt_df  = spark.read.parquet(unique_size_cnt_total_sum).select('cnt')
    cnt = cnt_df.sort(cnt_df.cnt.desc())
    cnt.write.csv(draw_repeat_cnt)


def draw_file_sum_size_by_cnt():
    #cnt_lst = []
    #cnt_array = np.loadtxt(open(unique_draw_cnt_data, "rb"))
    df = pd.read_csv(draw_sum_size_by_cnt)
    #with open(unique_draw_cnt_data) as f:
    #   for line in f:
    #       cnt_lst.append(line.replace("\n", ""))
    print("after loading file!")
    data = df.as_matrix()
    print(data)
    ylabel = 'Total size of files with same repeat count(MB)'
    xlabel = 'File repeat cnt'
    # data = [x * 1.0 / 1024 / 1024 for x in data1]
    xlim = data.max()
    ticks = 25
    fig = fig_size('min')
    print("xlim = %f", xlim)
    print(data.shape)
    plot_cdf_normal(fig, data, xlabel, xlim, ticks, ylabel)


def draw_file_avg_size_by_cnt():
    #cnt_lst = []
    #cnt_array = np.loadtxt(open(unique_draw_cnt_data, "rb"))
    df = pd.read_csv(draw_avg_size_by_cnt)
    #with open(unique_draw_cnt_data) as f:
    #   for line in f:
    #       cnt_lst.append(line.replace("\n", ""))
    print("after loading file!")
    data = df.as_matrix()
    print(data)
    ylabel = 'Average size of files with same repeat count(KB)'
    xlabel = 'File repeat count'
    # data = [x * 1.0 / 1024 / 1024 for x in data1]
    xlim = data.max()
    ticks = 25
    fig = fig_size('min')
    print("xlim = %f", xlim)
    print(data.shape)
    plot_cdf_normal(fig, data, xlabel, xlim, ticks, ylabel)


def draw_sum_file_size():
    #cnt_lst = []
    #cnt_array = np.loadtxt(open(unique_draw_cnt_data, "rb"))
    df = pd.read_csv(draw_sum_file_size_data)
    #with open(unique_draw_cnt_data) as f:
    #   for line in f:
    #       cnt_lst.append(line.replace("\n", ""))
    print("after loading file!")
    data = df.as_matrix()
    print(data)
    ylabel = 'Cumulative file probability'
    xlabel = 'Total size of redudant files with same content(KB)'
    # data = [x * 1.0 / 1024 / 1024 for x in data1]
    xlim = data.max()
    ticks = 25
    fig = fig_size('min')
    print("xlim = %f", xlim)
    print(data.shape)
    plot_cdf_normal(fig, data, xlabel, xlim, ticks, ylabel)


def draw_file_size():
    #cnt_lst = []
    #cnt_array = np.loadtxt(open(unique_draw_cnt_data, "rb"))
    df = pd.read_csv(unique_draw_size_data)
    #with open(unique_draw_cnt_data) as f:
    #   for line in f:
    #       cnt_lst.append(line.replace("\n", ""))
    print("after loading file!")
    data = df.as_matrix()
    print(data)
    ylabel = 'Cumulative file probability'
    xlabel = 'File size(KB)'
    # data = [x * 1.0 / 1024 / 1024 for x in data1]
    xlim = data.max()
    ticks = 25
    fig = fig_size('min')
    print("xlim = %f", xlim)
    print(data.shape)
    plot_cdf_normal(fig, data, xlabel, xlim, ticks, ylabel)


def draw_file_repeat_cnt():
    #cnt_lst = []
    #cnt_array = np.loadtxt(open(unique_draw_cnt_data, "rb"))
    df = pd.read_csv(unique_draw_cnt_data)
    #with open(unique_draw_cnt_data) as f:
    # 	for line in f:
    #	    cnt_lst.append(line.replace("\n", ""))
    print("after loading file!")
    data = df.as_matrix()
    print(data)
    ylabel = 'Cumulative file probability'
    xlabel = 'File repeat count'
    # data = [x * 1.0 / 1024 / 1024 for x in data1]
    xlim = data.max()
    ticks = 25
    fig = fig_size('min')
    print("xlim = %f", xlim)
    print(data.shape)
    plot_cdf_normal(fig, data, xlabel, xlim, ticks, ylabel)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
