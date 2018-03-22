from analysis_library import *
from pyspark.ml.clustering import *
from pyspark.ml.linalg import Vectors, VectorUDT

import numpy
import numpy as np
#import pandas as pd
import matplotlib.pyplot as plt
from pyspark.ml.feature import VectorAssembler, IDF, CountVectorizer
from pyspark.sql.column import _to_java_column, _to_seq, Column


def find_similar_filename(spark, sc):
	f_df = spark.read.parquet(uniq_1_copy_file_info).select('sha256', 'filename')#.limit(200)
	#split_filepath = F.udf((lambda s: s.lower().split('/')[1:]), ArrayType(StringType()))
	df = f_df.groupby('filename').agg(F.size(F.collect_set('sha256')).alias('uniq_cnt')).sort(col('uniq_cnt').desc())#.show()
	print("========>",df.count(), df.filter('uniq_cnt > 1').count())#.write.csv('/redundant_file_analysis/same_filename.csv')


def main():
	sc, spark = init_spark_cluster()
	find_similar_filename(spark, sc)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'