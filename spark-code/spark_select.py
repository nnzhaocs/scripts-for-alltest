
from analysis_library import *
from pyspark.ml.clustering import *
from pyspark.ml.linalg import Vectors, VectorUDT

import numpy
import numpy as np
#import pandas as pd
import matplotlib.pyplot as plt
from pyspark.ml.feature import VectorAssembler, IDF, CountVectorizer
from pyspark.sql.column import _to_java_column, _to_seq, Column


image_info = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_info.parquet')
image_basic_info = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_basic_info.parquet3')
image_layer_mapping_shared_pull_cnt = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_layer_mapping_shared_pull_cnt.parquet')
tarfilename = '/layers/sha256-000016103e3495feba2f646819de16502f47ab4fd85187b8a15ec5020f99897e-1515015621.9'


def save_popular_images():
	#sc, spark = init_spark_cluster()
	#info = spark.read.parquet(image_layer_mapping_shared_pull_cnt)#image_basic_info)#image_info)     
	"""
        df = spark.read.parquet(image_layer_mapping_shared_pull_cnt)
	image_layers = df.select('layer_id', 'image_realname')
	df = spark.read.parquet(image_info)#.select('pull_cnt', 'image_realname')#.filter("pull_cnt >= 0")
	df = df.select(df.pull_cnt.cast('float').alias('pull_cnt'), df.image_realname)
	df.show()
	pop_image = df.filter(df.pull_cnt >= 100)#.show()
	pop_image.write.csv('/redundant_image_analysis/pop_images.lst')
	"""
	"""
	#img_cnt = pop_image.count()
	pop_image_layers = pop_image.join(image_layers, ['image_realname'], 'inner')

	pop_image_layers.show()
	#cnt = pop_image_layers.select('layer_id').dropDuplicates().count()
	#spark.read.parquet(LAYER_FILE_MAPPING_DIR).show(20, False)
	#print("=====> %d:%d", cnt, img_cnt)
	pop_image_layers.write.csv('/redundant_layer_analysis/pop_image_layers.csv')
        """
	"""
	df = spark.read.parquet(LAYER_FILE_MAPPING_DIR).select('layer_id', 'digest')
	pop_files = df.join(pop_image_layers, ['layer_id'], 'inner')
	pop_files.show()
	pop_files = pop_files.select('digest')
	fle_cnt = pop_files.count()
	fle_uniq = pop_files.dropDuplicates().count()
	print("=====> %d:%d", fle_cnt, fle_uniq)
	"""
	plys = spark.read.csv('/redundant_layer_analysis/pop_image_layers.csv')
	pop_image_layers = plys.select(plys._c0.alias('image_realname'), plys._c1.alias('pull_cnt'), plys._c2.alias('layer_id'))	
	df = spark.read.parquet(layer_basic_info1, layer_basic_info2, layer_basic_info3).select('layer_id', 'compressed_size')
	pop_layer_size = df.join(pop_image_layers, ['layer_id'], 'inner')
	pop_layer_size.write.csv('/redundant_layer_analysis/pop_image_layer_size.csv')


def spark_get_10_pop_layers(spark, sc):
	df = spark.read.parquet(image_layer_mapping_shared_pull_cnt).select('layer_id', 'shared_image_cnt')
	df = df.sort(col('shared_image_cnt').desc()).dropDuplicates()
	df.show(20, False)

#def spark_get_uniq_files(spark, sc):
#	df = spark.read.parquet(LAYER_FILE_MAPPING_DIR).filter('cnt == 1')
	


def spark_get_10_pop_files(spark, sc):
	
	layer_db_df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
	ascii_utf = F.udf(lambda s: s.encode('utf-8'))
	layer_db_f = layer_db_df.withColumn('filename_new', ascii_utf('filename'))
	#.select('filename')
	#files = layer_db_df.selectExpr("explode(dirs) As structdirs").selectExpr(
        #"explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
	#files = layer_db_df.filter("digest = '68b329da9893e34099c7d8ad5cb9c940'")
	# d41d8cd98f00b204e9800998ecf8427e
	# dfdc3bad88b6e98080891d6323e2f58e
	# 5d0e769df33e016b3c52a0971e8d258c
	# a12ebca0510a773644101a99a867d210
	#3d10912d07e7bc8cd7d2faea51adb2d8
	#68b329da9893e34099c7d8ad5cb9c940
	func0 = F.udf(lambda s: max(s, key=len))
	empty_f = layer_db_f.groupby('digest').agg(F.size(F.collect_list('layer_id')).alias('cnt'), func0(F.collect_list('filename_new')).alias('filename')).select('filename', 'cnt').sort(col('cnt').desc())
	#empty_f.show(1000, False)
	empty_f.write.csv('/redundant_layer_analysis/pop_file1.csv')
	
	#files.show(14000, False)

	"""
	df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)#a.dropDuplicates('digest')#csv('/redundant_file_analysis/draw_type_by_repeat_cnt_layerdir.csv')
	#df_digest = df.select('digest').groupby('digest').agg(F.size(F.collect_list('digest')))
	#df_digest.show(20, False)
	df = df.dropDuplicates(['digest'])
	#df.show()
	
	file_info = spark.read.parquet(unique_file_basic_info).select(col('sha256').alias('digest'), 'cnt')
	file_info = file_info.sort(col('cnt').desc()).filter('cnt > 1000')
	df = df.join(file_info, ['digest'], 'inner').sort(col('cnt'))
	df.show(1000, False)
	#print("============> %d", df.count())
	"""
	
	
def spark_get_part_dedup_ratio(spark, sc):
	df = spark.read.parquet(os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_dup_ratio.parquet'))
	layer_ids = df.select('layer_id')
	#layer_dup_ratio = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_dup_ratio.parquet')
	
	lf_df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)#.select('layer_id').dropDuplicates(['layer_id'])
	df_rand = layer_ids.orderBy(F.rand()).limit(10000)#.show() # 1,000; 10,000; 100,000; 1000,000;
	df_1 = lf_df.join(df_rand, ['layer_id'], 'inner')

	df_f_info = spark.read.parquet(unique_file_basic_info).select(col('sha256').alias('digest'), 'avg')
	df_sple = df_f_info.join(df_1, ['digest'], 'inner')

	df_s_sum = df_sple.select(F.sum('avg')).collect()
	
	df_1_uniq = df_sple.dropDuplicates(['digest'])	
	
	df_s_uniq = df_1_uniq.select(F.sum('avg')).collect()
	print("==============> total cnt: %d, uniq cnt: %d", df_1.count(), df_1_uniq.count())
	print df_s_sum
	print df_s_uniq

def spark_process_tarball(spark, sc):
	df = sc.textFile(tarfilename)
	print("=============> %d", df.count())
	print df.take(10)


def as_vector(sc, col):
	f = sc._jvm.com.example.spark.udfs.udfs.as_vector()
	return Column(f.apply(_to_seq(sc, [col], _to_java_column)))

def fill_with_null(col):
	"""
	if len(col) < :
		for i in range():
			col.append(None)
	"""
def find_similar_filename(spark, sc):
	f_df = spark.read.parquet(uniq_1_copy_file_info).select('sha256', 'filename')#.limit(200)
	#split_filepath = F.udf((lambda s: s.lower().split('/')[1:]), ArrayType(StringType()))
	df = f_df.groupby('filename').agg(F.size(F.collect_set('sha256')).alias('uniq_cnt')).sort(col('uniq_cnt').desc())#.show()
	print("========>",df.count(), df.filter('uniq_cnt > 1').count())#.write.csv('/redundant_file_analysis/same_filename.csv')


def km_clustering_filename(spark, sc):
	f_df = spark.read.parquet(uniq_1_copy_file_info).select('sha256', 'filename').limit(200)

	split_filepath = F.udf((lambda s: s.lower().split('/')[1:]), ArrayType(StringType()))

	df = f_df.withColumn('new_name', split_filepath('filename'))
	df.show(10, False)
	
	#vector_transfer = F.udf(lambda s: Vectors.dense(s), VectorUDT())
	#df = df.withColumn('vector', vector_transfer('new_name')).select('sha256', 'vector')

	cv = CountVectorizer(inputCol = "new_name", outputCol = "rawFeatures", vocabSize = 1000)
	cvmodel = cv.fit(df)
	featurizedData = cvmodel.transform(df).na.drop()
	featurizedData.show()
	
	vocab = cvmodel.vocabulary
	#print vocab
	
	vocab_broadcast = sc.broadcast(vocab)

	idf = IDF(inputCol = 'rawFeatures', outputCol = "features")
	idfModel = idf.fit(featurizedData)
	rescaleData = idfModel.transform(featurizedData).na.drop()

	rescaleData.show(10, False)
	
	lda = LDA(k = 2)#, seed = 5, optimizer="em", featuresCol = "features")
	ldamodel = lda.fit(rescaleData)#.transform(rescaleData)
	#ldamodel.show(10, False)
	ldamodel.isDistributed()
	ldamodel.vocabSize()

	ldatopics = ldamodel.describeTopics()
	ldatopics.show(10, False)

	#func0 = F.udf((lambda s, vocab: vocab[i] for i in s), ArrayType(StringType()))

	#ldatopics.withColumn('term', func0(vocab, col('termIndices'))).show(10, False)
	"""
	topics = ldamodel.topicsMatrix()

	for topic in range(5):
		print("Topic " + str(topic) + ":")
		for word in range(0, ldamodel.vocabSize()):
        		print(" " + str(topics[word][topic]))
	
	"""
	"""
        cost = np.zeros(20)
        for k in range(2, 20):
                kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol('vector')
                model = kmeans.fit(df)#.sample(False, 0.1, seed=42))
                cost[k] = model.computeCost(df)
        
        fig, ax = plt.subplot(1, 1, figsize = (8, 6))
        ax.plot(range(2, 20), cost[2:20])
        ax.set_xlabel('k')
        ax.set_ylabel('cost')
	"""

def spark_process_split(spark, sc):
	lf_df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)

	hex_to_decimal = F.udf(lambda s: int(s, 16))

	lf_df = lf_df.withColumn('digest_int', hex_to_decimal('digest'))
	#lf_df = lf_df.select('layer_id', col('digest').cast('float').alias('digest'))	

	df = lf_df.groupby('layer_id').agg(F.size(F.collect_list('digest_int')).alias('cnt'))
	#df.select(F.max('cnt')).show()

	df = lf_df.groupby('layer_id').agg(F.collect_list('digest_int').alias('features'))
	
	vector_transfer = F.udf(lambda s: Vectors.dense(s), VectorUDT())
	df = df.withColumn('vector', vector_transfer('features'))	
	#df.show(2, False)
	df = df.select('layer_id', 'vector')
        df.show(10, False)
	"""
	#df.show(2, False)
	#with_vec = df.withColumn('vector', as_vector(sc, 'features'))	
	#with_vec.show()
	#df.show()
	#FEATURES_COL = ['features']
	#vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
	#df = vecAssembler.transform(df).select('layer_id', 'features')
	#df.show(2, False)
	
	cost = np.zeros(20)
	for k in range(2, 20):
		kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol('vector')
		model = kmeans.fit(df)#.sample(False, 0.1, seed=42))
		cost[k] = model.computeCost(df)
	
	fig, ax = plt.subplot(1, 1, figsize = (8, 6))
	ax.plot(range(2, 20), cost[2:20])
	ax.set_xlabel('k')
	ax.set_ylabel('cost')
	"""

def main():
	sc, spark = init_spark_cluster()
	#save_popular_images()
	#spark_process_tarball(spark, sc)
	#spark_get_10_pop_layers(spark, sc)
	#spark_get_10_pop_files(spark, sc)
	#spark_get_part_dedup_ratio(spark, sc)
	#spark_process_split(spark, sc)
	#km_clustering_filename(spark, sc)
	find_similar_filename(spark, sc)

"""
def calculate_compression():
	#sc, spark = init_spark_cluster()
	df = spark.read.parquet(image_layer_mapping_shared_pull_cnt).select('layer_id', 'shared_image_cnt').dropDuplicates()
	df.show()
	layer_info = spark.read.parquet(layer_basic_info1, layer_basic_info2, layer_basic_info3).select(
        'layer_id', 'compressed_size')
	layer_info.show()
	layer_size_before = df.join(layer_info, ['layer_id'], 'inner')
	#layer_size_before.show()
	size_before = layer_size_before.withColumn('size_before', F.col('shared_image_cnt')*F.col('compressed_size'))
	#size_before.show()
	size_before.select(F.sum('size_before')).show()
	size_before.select(F.sum('compressed_size')).show()
"""
if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
