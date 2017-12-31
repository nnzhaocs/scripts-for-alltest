
import sys
sys.path.append('../plotter/')
from draw_pic import *
from analysis_library import *


unique_size_cnt_total_sum = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_size_cnt_total_sum.parquet')

def main():
    sc, spark = init_spark_cluster()



def plot_file_repeat_cnt(spark, sc):
    cnt_df  = spark.read.parquet(unique_size_cnt_total_sum).select('cnt')
    cnt = cnt_df.sort(cnt_df.cnt.desc()).collect()
    cnt_lst = [str(i.value) for i in cnt]

    data = cnt_lst
    ylabel = 'Cumulative file repeat count probability'
    xlabel = 'File repeat count'
    # data = [x * 1.0 / 1024 / 1024 for x in data1]
    xlim = max(data)
    ticks = 25
    fig = fig_size('small')  # 'large'
    print "mean = %f, len = %d"%(xlim,len(data))
    plot_cdf_normal(fig, data, xlabel, xlim, ticks, ylabel)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'