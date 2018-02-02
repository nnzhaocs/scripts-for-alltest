
from analysis_library import *

image_info = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_info.parquet')
image_basic_info = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_basic_info.parquet3')
image_layer_mapping_shared_pull_cnt = os.path.join(REDUNDANT_IMAGE_ANALYSIS_DIR, 'image_layer_mapping_shared_pull_cnt.parquet')



def main():
	sc, spark = init_spark_cluster()
	info = spark.read.parquet(image_layer_mapping_shared_pull_cnt)#image_basic_info)#image_info)
	print("=====> %d", info.count())
	







if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
