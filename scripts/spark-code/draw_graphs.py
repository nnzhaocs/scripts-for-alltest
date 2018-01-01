
import sys
sys.path.append('../plotter/')
from draw_pic import *
#from analysis_library import *
import pandas as pd

#unique_size_cnt_total_sum = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_size_cnt_total_sum.parquet')
#unique_draw_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_draw_cnt.csv')

RESULTS_DIR = '/home/nannan/4tb_results'
unique_draw_cnt_data = os.path.join(RESULTS_DIR, 'unique_draw_cnt_data.csv')

def main():
    #sc, spark = init_spark_cluster()
    #save_file_repeat_cnt(spark, sc)
    draw_file_repeat_cnt()


def save_file_repeat_cnt(spark, sc):
    cnt_df  = spark.read.parquet(unique_size_cnt_total_sum).select('cnt')
    cnt = cnt_df.sort(cnt_df.cnt.desc())#.collect()
    #cnt_lst = [str(i.value) for i in cnt]
    cnt.write.csv(unique_draw_cnt)


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
    ylabel = 'Cumulative file repeat\n count probability'
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
