from analysis_library import *
from pyspark.ml.clustering import *
from pyspark.ml.linalg import Vectors, VectorUDT

import numpy
import numpy as np
#import pandas as pd
import matplotlib.pyplot as plt
from pyspark.ml.feature import VectorAssembler, IDF, CountVectorizer
from pyspark.sql.column import _to_java_column, _to_seq, Column
#from get_file_type import *
import get_file_type


def find_similar_filename(spark, sc):
	f_df = spark.read.parquet(uniq_1_copy_file_info).select('sha256', 'filename')#.limit(200)
	#split_filepath = F.udf((lambda s: s.lower().split('/')[1:]), ArrayType(StringType()))
	df = f_df.groupby('filename').agg(F.size(F.collect_set('sha256')).alias('uniq_cnt')).sort(col('uniq_cnt').desc())#.show()
	#print("========>",df.count())#
	df.write.csv('/redundant_file_analysis/whole_same_filename.csv')
	df.filter('uniq_cnt == 1').write.csv('/redundant_file_analysis/same_filename_uniq_name.csv')
	#df.filter('uniq_cnt > 1').select(F.sum('uniq_cnt')).show()


def same_filenames_type(spark, sc):
	df = spark.read.csv('/redundant_file_analysis/same_filename.csv').select(col('_c0').alias('filename'), col('_c1').alias('uniq_cnt'))
	f_df = spark.read.parquet(uniq_1_copy_file_info)
	df = df.join(f_df, ['filename'], 'inner')
	df.write.save('/redundant_file_analysis/same_filename_info.csv')


def group_by_lib(spark, sc):
	df = spark.read.parquet('/redundant_file_analysis/dirs_speci_type_same_filenames.parquet')
	df.where((col('dir3') == 'lib')| (col('dir2') == 'lib')).show()
	#df.show(2)


def group_by_1st_dir(spark, sc):
	df = spark.read.parquet('/redundant_file_analysis/dirs_speci_type_same_filenames.parquet')
	comm_dir = [
	'var',
	'etc',
	'root',
	'usr',
	#'app',
	'opt',
	'run',
	#'code',
	#'node_modules',
	'tmp',
	'.git',
	'srv',
	'build'
	]
	#df.show()
	df_noncomm_dir = df.filter(~col('dir1').isin(comm_dir))
	get_extension = F.udf(lambda s: os.path.splitext(s)[1])
	df_extension = df_noncomm_dir.withColumn('extension', get_extension('filename'))
	df_extension.write.save('/redundant_file_analysis/draw_file_extensions.parquet')
	#df_extension.groupby('extension').agg(F.sum('uniq_cnt_total').alias('cnt')).sort(col('cnt').desc()).write.csv('/redundant_file_analysis/draw_group_extension.csv')
	#df = spark.read.csv('/redundant_file_analysis/draw_group_nondir.csv').select(F.sum('_c2'))
	#df.show()
	#df.printSchema()
	#df.write.csv('/redundant_file_analysis/draw_group_nondir_order.csv')
	
	#df = spark.read.csv('/redundant_file_analysis/draw_group_dir1.csv')
	#df.write.save('/redundant_file_analysis/draw_group_dir1.csv')
	#df = spark.read.csv('/redundant_file_analysis/draw_type_speci_same_filenames.csv')
	#df.show()


def group_by_library_software(spark, sc):
	df = spark.read.parquet('/redundant_file_analysis/dirs_speci_type_same_filenames.parquet')
        comm_dir = [
        'var',
        'etc',
        'root',
        'usr',
        #'app',
        'opt',
        'run',
        #'code',
        #'node_modules',
        'tmp',
        '.git',
        'srv',
        'build'
        ]
	df_comm_dir = df.filter(col('dir1').isin(comm_dir))
        get_extension = F.udf(lambda s: os.path.splitext(s)[1])
        df_extension = df_comm_dir.withColumn('extension', get_extension('filename'))
	df_extension.filter(col('uniq_cnt_total') == 1).show(20, False)
	#df_extension.select(F.sum('uniq_cnt_total')).show()
        #df_extension.write.save('/redundant_file_analysis/draw_comm_file_extensions.parquet')
	#df_extension.groupby('extension').agg(F.sum('uniq_cnt_total').alias('cnt')).sort(col('cnt').desc()).write.csv('/redundant_file_analysis/draw_group_comm_dir_extension.csv')


def get_src_extension(spark, sc):
	extensions = [
	'.js',
	'.cmd',
	'.rb',
	'.cs',
	'.cs',
	'.make',
	'.cmake',
	'.go',
	'.java',
	'.sh',
	'.py',
	'.h',
	'.html',
	'.yaml',
	'.conf',
	'.xml',
	'.prl',
	'.c'
	]
	df = spark.read.parquet('/redundant_file_analysis/draw_file_extensions.parquet')
	df = df.filter(col('extension').isin(extensions))#.show()
	df.groupby(['dir1', 'dir2', 'dir3']).agg(F.sum('uniq_cnt_total').alias('cnt')).sort(col('cnt').desc()).write.csv('/redundant_file_analysis/draw_type_speci_dir3.csv')

	

def group_by_filenames(spark, sc):
	df = spark.read.csv('/redundant_file_analysis/draw_type_speci_same_filenames.csv').select(col('_c0').alias('filename'), col('_c1').cast('float').alias('sum_size'),
		col('_c2').cast('float').alias('uniq_cnt_total'))
	#df.show()
	split_fpath = F.udf(lambda s: s.lower().split('/')[1:][0] if len(s.lower().split('/')[1:]) == 1 else None)
	df = df.withColumn('dir1', split_fpath('filename')).filter(col('dir1').isNotNull())
	df = df.groupby('dir1').agg(F.size(F.collect_list('filename')).alias('uniq_fname_cnt'), F.sum('uniq_cnt_total').alias('uniq_cnt_total'), F.sum('sum_size').alias('sum_size_total')).sort(col('uniq_cnt_total').desc())
	df.write.csv('/redundant_file_analysis/draw_group_nondir.csv')	


def group_by_dirs(spark, sc):
	df = spark.read.parquet('/redundant_file_analysis/dirs_speci_type_same_filenames.parquet')
	#df.show()
	df = df.filter(col('dir1').isNotNull())
	df = df.filter(col('dir2').isNotNull())
	df = df.filter(col('dir3').isNotNull())
	#df = df.filter(col('dir4').isNotNull())
	#df = df.filter(col('dir5').isNotNull())
	
	#df = df.filter(col('dir6').isNotNull())
	#df = df.filter(col('dir7').isNotNull())
	#df = df.filter(col('dir8').isNotNull())
	#df = df.filter(col('dir9').isNotNull())
	df = df.groupby(['dir1', 'dir2', 'dir3']).agg(F.size(F.collect_list('filename')).alias('uniq_fname_cnt'), F.sum('uniq_cnt_total').alias('uniq_cnt_total'), F.sum('sum_size').alias('sum_size_total')).sort(col('uniq_cnt_total').desc())
	df.write.csv('/redundant_file_analysis/dirs_by_123_all__speci_type_same_filenames.csv')#show(10, False)	
	print("======>", df.count())
	#df.show(10, False)

"""
def get_filename_dirs(spark, sc):
	#df = spark.read.csv('/redundant_file_analysis/draw_group_dir1.csv').select(col('_c0').alias(''))
        df = spark.read.csv('/redundant_file_analysis/draw_type_speci_same_filenames.csv').select(col('_c0').alias('filename'), 
		col('_c1').alias('sum_size'), col('_c2').alias('uniq_cnt_total'))
	#df.show()
	split_fpath = F.udf(lambda s: len(s.lower().split('/')[1:-1]))
	df = df.withColumn('dir_depth', split_fpath('filename'))
	#df.select(F.max('dir_depth')).show()
	#df.filter('dir_depth == 9').show(10, False) #9
	split_fpath1 = F.udf(lambda s: s.lower().split('/')[1:-1][0] if len(s.lower().split('/')[1:-1]) >= 1 else None)
	split_fpath2 = F.udf(lambda s: s.lower().split('/')[1:-1][1] if len(s.lower().split('/')[1:-1]) >= 2 else None)
	split_fpath3 = F.udf(lambda s: s.lower().split('/')[1:-1][2] if len(s.lower().split('/')[1:-1]) >= 3 else None)
	split_fpath4 = F.udf(lambda s: s.lower().split('/')[1:-1][3] if len(s.lower().split('/')[1:-1]) >= 4 else None)
        split_fpath5 = F.udf(lambda s: s.lower().split('/')[1:-1][4] if len(s.lower().split('/')[1:-1]) >= 5 else None)
        split_fpath6 = F.udf(lambda s: s.lower().split('/')[1:-1][5] if len(s.lower().split('/')[1:-1]) >= 6 else None)
        split_fpath7 = F.udf(lambda s: s.lower().split('/')[1:-1][6] if len(s.lower().split('/')[1:-1]) >= 7 else None)
        split_fpath8 = F.udf(lambda s: s.lower().split('/')[1:-1][7] if len(s.lower().split('/')[1:-1]) >= 8 else None)
	split_fpath9 = F.udf(lambda s: s.lower().split('/')[1:-1][8] if len(s.lower().split('/')[1:-1]) >= 9 else None)

	df = df.withColumn('dir1', split_fpath1('filename'))
	df = df.withColumn('dir2', split_fpath2('filename'))
	df = df.withColumn('dir3', split_fpath3('filename'))
	df = df.withColumn('dir4', split_fpath4('filename'))
	df = df.withColumn('dir5', split_fpath5('filename'))
	df = df.withColumn('dir6', split_fpath6('filename'))
	df = df.withColumn('dir7', split_fpath7('filename'))
	df = df.withColumn('dir8', split_fpath8('filename'))
	df = df.withColumn('dir9', split_fpath9('filename'))

	df.write.parquet('/redundant_file_analysis/dirs_speci_type_same_filenames.parquet')

types = [
	'ascii text',
	'utf-8 unicode text',
	'assembler source',
	'ahtml-xml-xhtml-doc',
	'abash-shell-script',
	'ac-c--source',
	'amakefile script',
	'apython-script',
	'anon-iso extended-ascii text',
	'aphp script',
	'aiso-8859 text',
	'afortran program',
	'aother-script',
	'autf-8 unicode',
	'aperl-pod-doc',
	'aruby module source',
	'aawk script',
	'aruby script',
	'anode-script',
	'aperl5 module source',
	'apascal source',
	'aperl-script',
	'ainternational ebcdic text',
	'aautomake makefile script',
	'aapplesoft basic program data',
	'alisp/scheme program',
	'afortran programdata',
	'am4 macro processor script',
	'alinux make config build file',
	'aphp-script',
	'atcl script',
	'akde config file'
]
"""
def group_files_by_filenames(spark, sc):
        file_info = spark.read.parquet('/redundant_file_analysis/same_filename_info.csv')#.filter(F.size('type') > 0)

        #func = F.udf(lambda s: s[0]#.replace("sticky ", "").replact("posix ", "").replace("setgid ", "").replace("setuid ", "").replace("compiled ", "").split(" ")[0])
        func0 = F.udf(filter_whole_types)
        #func1 = F.udf(lambda s: s[0] if len(s) else None)

        file_info_df = file_info.select(func0('type').alias('type'),
                'sha256', 'avg', 'uniq_cnt', 'filename')
        file_info_df = file_info_df.filter(file_info_df.type.isin(types))
        file_info_df.printSchema()
        """
        sort_cnt = file_info_df.sort(file_info_df.cnt.desc())
        sort_cnt.show()
        sort_cnt.write.csv(draw_type_by_repeat_cnt)

        sort_cap = file_info_df.sort(file_info_df.total_sum.desc())
        sort_cap.show()
        sort_cap.write.csv(draw_type_by_total_sum)
        """

        type1 = file_info_df.groupby('filename').agg(F.sum('avg').alias('sum_size'), F.sum('uniq_cnt').alias('uniq_cnt_total'))
        type1.printSchema()

        #type1 = type1.withColumn('total_cnt', F.col('_cnt') + F.col('total_cnt'))
        #type1 = type1.withColumn('dup_ratio_cap', (F.col('sum_size') - F.col('size_dropdup'))/F.col('sum_size')*1.0)
        #type1 = type1.withColumn('dup_ratio_cnt', (F.col('total_cnt') - F.col('uniq_cnt'))* 1.0 / F.col('total_cnt'))

        #sort_type_cnt = type1.sort(type1.total_cnt.desc())
        #sort_type_cnt.show()
        #sort_type_cnt.coalesce(400).write.csv('/redundant_file_analysis/draw_type1_by_cnt_med.csv')#draw_type1_by_repeat_cnt)

        #sort_type_size = type1.sort(type1.uniq_cnt_total.desc())
        #sort_type_size.show()
        type1.coalesce(400).write.csv('/redundant_file_analysis/draw_type_speci_same_filenames.csv')#draw_type_by_size)


def save_by_type(spark, sc):
	"""save by repeat cnt"""
	file_info = spark.read.parquet('/redundant_file_analysis/same_filename_info.csv')#.filter(F.size('type') > 0)

	#func = F.udf(lambda s: s[0]#.replace("sticky ", "").replact("posix ", "").replace("setgid ", "").replace("setuid ", "").replace("compiled ", "").split(" ")[0])
	func0 = F.udf(filter_whole_types)
	#func1 = F.udf(lambda s: s[0] if len(s) else None)

	file_info_df = file_info.select(func0('type').alias('type'),
                'sha256', 'avg', 'uniq_cnt')
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

	type1 = file_info_df.groupby('type').agg(F.sum('avg').alias('sum_size'), F.sum('uniq_cnt').alias('uniq_cnt_total'))
	type1.printSchema()
	
	#type1 = type1.withColumn('total_cnt', F.col('_cnt') + F.col('total_cnt'))
	#type1 = type1.withColumn('dup_ratio_cap', (F.col('sum_size') - F.col('size_dropdup'))/F.col('sum_size')*1.0)
	#type1 = type1.withColumn('dup_ratio_cnt', (F.col('total_cnt') - F.col('uniq_cnt'))* 1.0 / F.col('total_cnt'))

	#sort_type_cnt = type1.sort(type1.total_cnt.desc())
	#sort_type_cnt.show()
	#sort_type_cnt.coalesce(400).write.csv('/redundant_file_analysis/draw_type1_by_cnt_med.csv')#draw_type1_by_repeat_cnt)

	sort_type_size = type1.sort(type1.uniq_cnt_total.desc())
	#sort_type_size.show()
	sort_type_size.coalesce(400).write.csv('/redundant_file_analysis/draw_type_2_by_cap_total.csv')#draw_type_by_size)
	"""
	sort_type_dup_r = type1.sort(type1.dup_ratio_cap.desc())
	#sort_type_dup_r.show()
	sort_type_dup_r.write.csv(draw_type_by_dup_ratio_cap)

	sort_type_dup_r = type1.sort(type1.dup_ratio_cnt.desc())
	#sort_type_dup_r.show()
	sort_type_dup_r.write.csv(draw_type_by_dup_ratio_cnt)
	"""


def main():
	sc, spark = init_spark_cluster()
	#group_by_1st_dir(spark, sc)
	#find_similar_filename(spark, sc)
	#same_filenames_type(spark, sc)
	#save_by_type(spark, sc)
	#group_files_by_filenames(spark, sc)
	#group_by_filenames(spark, sc)
	#group_by_dirs(spark, sc)
	#get_src_extension(spark, sc)
	#group_by_lib(spark, sc)
	group_by_library_software(spark, sc)
	

if __name__ == '__main__':
    print('start!')
    main()
    print('finished!')
