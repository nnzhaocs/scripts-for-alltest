
from analysis_library import *


unique_file_digest = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_file_digest.parquet')
unique_file_cnts = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_file_cnts.parquet')
unique_cnt_size = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_cnt_size.parquet')
unique_size_cnt_total_sum = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'unique_size_cnt_total_sum.parquet')


def main():

    sc, spark = init_spark_cluster()
    save_unique_file_digests(spark, sc)
    save_unique_files_cnts(spark, sc)
    save_unique_files_size(spark, sc)
    save_unique_file_size_infos(spark, sc)


def save_unique_file_size_infos(spark, sc):
    cnt_size_df = spark.read.parquet(unique_cnt_size)
    cnt_size = cnt_size_df.groupby('sha256').agg(collect_list('`file_info.stat_size`').alias('file_size'),
                                                 F.sum('`file_info.stat_size`').alias('total_sum'),
                                                 F.avg('`file_info.stat_size`').alias('avg'),
                                                 F.size(collect_list('`file_info.stat_size`')).alias('cnt'))
    cnt_size.show()
    cnt_size.write.save(unique_size_cnt_total_sum)


def save_unique_files_size(spark, sc):
    cnt_unique = spark.read.parquet(unique_file_cnts)

    layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
    files = layer_db_df.selectExpr("explode(dirs) As structdirs").selectExpr(
        "explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
    regular_files = files.filter(files.sha256.isNotNull())

    unique_df = regular_files.join(cnt_unique, ['sha256'], 'leftsemi')
    unique_df.show()
    cnt_size = unique_df.select(unique_df.sha256, unique_df.file_info.stat_size)
    cnt_size.write.save(unique_cnt_size)


"""save uniq file cnts"""
def save_unique_files_cnts(spark, sc):
    layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
    files = layer_db_df.selectExpr("explode(dirs) As structdirs").selectExpr(
        "explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
    regular_files = files.filter(files.sha256.isNotNull())

    cnt = regular_files.groupby(regular_files.sha256).count().distinct()
    cnt.write.save(unique_file_cnts)


"""save uniqe file digests"""
def save_unique_file_digests(spark, sc):
    layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR)
    files = layer_db_df.selectExpr("explode(dirs) As structdirs").selectExpr(
        "explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
    regular_files = files.filter(files.sha256.isNotNull())
    uniq_files = regular_files.select(regular_files.sha256).dropDuplicates()
    """file_group = regular_files \
		.agg(collect_list(regular_files.sha256) \
		.alias("ids")) \
		.where(size("ids") > 1) """
    uniq_files.show()
    uniq_files.write.save(unique_file_digest)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'