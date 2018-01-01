
import sys
sys.path.append('../plotter/')
from draw_pic import *
#from analysis_library import *
import pandas as pd

#unique_size_cnt_total_sum = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_size_cnt_total_sum.parquet')
#unique_draw_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_draw_cnt.csv')

RESULTS_DIR = '/home/nannan/4tb_results'
#unique_draw_cnt_data = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_draw_cnt_data.csv')
#unique_draw_size = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_draw_size.csv')
#draw_sum_file_size = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_sum_file_size.csv')
#avg_size_by_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'avg_size_by_cnt.parquet')
#draw_avg_size_by_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_avg_size_by_cnt')
#draw_sum_size_by_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_sum_size_by_cnt')

unique_draw_cnt_data = os.path.join(RESULTS_DIR, 'unique_draw_cnt_data.csv')
unique_draw_size_data = os.path.join(RESULTS_DIR, 'unique_draw_size_data.csv')
draw_sum_file_size_data = os.path.join(RESULTS_DIR, 'draw_sum_file_size_data.csv')
#avg_size_by_cnt = os.path.join(RESULTS_DIR, 'avg_size_by_cnt.parquet')
draw_avg_size_by_cnt = os.path.join(RESULTS_DIR, 'draw_avg_size_by_cnt.csv')
draw_sum_size_by_cnt = os.path.join(RESULTS_DIR, 'draw_sum_size_by_cnt.csv')


def main():
    #sc, spark = init_spark_cluster()
    #save_file_repeat_cnt(spark, sc)
    #save_file_size(spark, sc)
    #save_sum_file_size(spark, sc)
    #save_avg_size_by_repeat_cnt(spark, sc)
    #draw_file_repeat_cnt()
    draw_file_size()
    draw_sum_file_size()


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

    new_df = df_2.withColumn("sum_size_1", df_1) 

    df_2 = df.filter(df.cnt > 1)
    df_2.show()
    df_2 = df_2.select(F.sum('total_sum').alias('sum_size_2_redundancy'))
    df_2.show()

    new_df = new_df.withColumn('sum_size_1', df_2)

    df = df.select(F.sum('avg').alias('sum_files_remove_redundancy'))
    df.show()

    new_df = new_df.withColumn('sum_files_remove_redundancy', df)

    df = df.select(F.sum('total_sum').alias('sum_files_redundancy'))
    df.show()

    new_df = new_df.withColumn('sum_files_redundancy', df)


def save_file_size(spark, sc):
    size_df = spark.read.parquet(unique_size_cnt_total_sum).select((F.col('avg')/1024.0).alias('avg')) #kb
    size_df.show()
    size = size_df.sort(size_df.avg)
    size.coalesce(1).write.csv(unique_draw_size)


def save_avg_size_by_repeat_cnt(spark, sc):
    df = spark.read.parquet(unique_size_cnt_total_sum)
    size_cnt = df.groupby('cnt').agg(F.sum('avg').alias('sum_size'), F.size(collect_list('avg')).alias('size_cnt'))
    size_cnt = size_cnt.withColumn('avg_size_by_cnt', (F.col('sum_size')/F.col('size_cnt')/1024.0)) #kb
    size_cnt = size_cnt.sort(size_cnt.cnt)
    size_cnt.write.save(avg_size_by_cnt)
    df = size_cnt.select('cnt', 'avg_size_by_cnt')
    df.coalesce(1).write.csv(draw_avg_size_by_cnt)
    df = size_cnt.select('cnt', (F.col('sum_size')/1024.0/1024.0).alias('sum_size')) #mb
    df.coalesce(1).write.csv(draw_sum_size_by_cnt)
    

def save_sum_file_size(spark, sc):
    df_f = spark.read.parquet(unique_size_cnt_total_sum).select((F.col('total_sum')/1024.0/1024.0).alias('total_sum')) #mb
    df = df_f.sort(df_f.total_sum)
    df.write.csv(draw_sum_file_size)


def save_file_repeat_cnt(spark, sc):
    cnt_df  = spark.read.parquet(unique_size_cnt_total_sum).select('cnt')
    cnt = cnt_df.sort(cnt_df.cnt.desc())#.collect()
    #cnt_lst = [str(i.value) for i in cnt]
    cnt.write.csv(unique_draw_cnt)


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
