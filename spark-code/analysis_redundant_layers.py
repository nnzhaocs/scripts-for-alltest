
from analysis_library import *
import time

layer_file_cnt = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_file_cnt.parquet')
unique_file_layer_mapping = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'unique_file_layer_mapping.parquet')
layer_dup_ratio = os.path.join(REDUNDANT_LAYER_ANALYSIS_DIR, 'layer_dup_ratio.parquet')
test_layers = os.path.join('/local/layer.json.lst_total')
res_list = '/local/operation_time.csv'
total_res_time = '/redundant_layer_analysis/total.layer_ids.ops.times_nonempty-1'
res_5_list = '/local/opertation_time_5.csv'

list_elem_num = 60


def main():

    sc, spark = init_spark_cluster()
    #get_res_layer_id(spark, sc)
    get_layer_res_size(spark, sc)
    #save_unique_file_layer_mapping(spark, sc)
    #save_layer_file_cnt(spark, sc)
    #find_file_digest_in_layer(spark, sc)
    #save_layer_redundant_info(spark, sc)
    #save_indexing_files_layer(spark, sc)
    #save_layer_uniq_shared_size(spark, sc)
    #save_layer_info(spark, sc)
    # layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR).dropDuplicates('layer_id')
    # files = layer_db_df.selectExpr("explode(dirs) As structdirs").selectExpr(
    #     "explode(structdirs.files) As structdirs_files").selectExpr("structdirs_files.*")
    # regular_files = files.filter(files.sha256.isNotNull())
    #parse_res_time(spark, sc)
    #save_specifi_layer_dup_ratio(spark, sc)

def get_res_layer_id(spark, sc):
    df = spark.read.csv(total_res_time).select('_c0')
    func0 = F.udf(lambda s: 'sha256:'+re.split(';', s)[0])
    func1 = F.udf(lambda s: re.split(';', s)[1])
    func2 = F.udf(lambda s: re.split(';', s)[2])
    df = df.withColumn('layer_id', func0('_c0'))
    df = df.withColumn('ops', func1('_c0'))
    df = df.withColumn('latency', func2('_c0'))

    df.write.parquet('/redundant_layer_analysis/res_layer_ids.parquet')

def get_layer_res_size(spark, sc):
    df = spark.read.parquet(layer_basic_info1, layer_basic_info2, layer_basic_info3).select(
        'layer_id', 'archival_size', 'compressed_size')
    #df.show()
    res_df = spark.read.parquet('/redundant_layer_analysis/res_layer_ids.parquet').select('layer_id', 'ops', 'latency')
    new_df = res_df.join(df, ['layer_id'], 'inner')
    new_df.filter("ops = 'decompress'").write.csv('/redundant_layer_analysis/res_layer_id_decompress.csv')
    new_df.filter("ops = 'extract'").write.csv('/redundant_layer_analysis/res_layer_id_extract.csv')
    new_df.filter("ops = 'compress'").write.csv('/redundant_layer_analysis/res_layer_id_compress.csv')
    new_df.filter("ops = 'digest'").write.csv('/redundant_layer_analysis/res_layer_id_digest.csv')
    new_df.filter("ops = 'archival'").write.csv('/redundant_layer_analysis/res_layer_id_archival.csv')
    

def parse_res_time(spark, sc):
    """
    lst = spark.read.csv(res_list)
    total_res = lst.groupby('_c0').agg(F.size(F.collect_set('_c1')).alias('ops'), F.sum('_c2'))
    layer_id = total_res.filter('ops == 6')#.write.csv('/local/total_res_sum.csv')
    tested_res = lst.join(layer_id, ['_c0'], 'inner')
    tested_res.write.csv('/local/tested_res.csv')
    """
    lst = spark.read.csv('/local/tested_res.csv')
    lst = lst.where((F.col('_c1') == 'compress') | (F.col('_c1') == 'archival'))
    lst.groupby('_c0').agg(F.sum('_c2')).write.csv('/local/light_pulling.csv')
    """
    dup_list = spark.read.csv(res_5_list)
    total_res = dup_list.groupby('_c0').agg(F.size(F.collect_set('_c1')).alias('ops'), F.sum('_c2'))
    #layer_id = 
    total_res.filter('ops == 5').write.csv('/local/total_res_5_sum.csv')
    """
def split_list(datalist):

    sublists = [datalist[x:x+list_elem_num] for x in range(0, len(datalist), list_elem_num)]

    #print(sublists)
    return sublists


#def load_json_files(absfilename_list, spark):
#
#    #print(absfilename_list)
#    sublists = split_list(absfilename_list)
#
#    for sublist in sublists:
#        load_subset_json_files(sublist, spark)
#        print("===========================>finished one sublist!!!!!!!!!")


def load_subset_json_files(sublist, spark, df):
    start = time.time()
    df.filter(df.layer_id.isin(sublist))#.coalesce(40000).write.save(output_absfilename, format="parquet", mode='append')
    elapsed = time.time() - start
    print('consumed time ==> %f s', elapsed/60)
    #print('FINISHED! to ==========> %s', layer_dir)


def save_indexing_files_layer(spark, sc):
    #layer_lst = spark.read.csv(test_layers)
    absfilename_list = spark.read.text(test_layers).collect()
    absfilenames = [str(i.value) for i in absfilename_list]
    df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)

    sublists = split_list(absfilenames)

    for sublist in sublists:
        load_subset_json_files(sublist, spark, df)
        print("===========================>finished one sublist!!!!!!!!!")
   

def save_layer_info(spark, sc):
    layer_db_df = spark.read.parquet(layer_db_absfilename3)#.dropDuplicates('layer_id')
    layerinfo_df = layer_db_df.select('layer_id', layer_db_df.size.archival_size.alias('archival_size'),
                             layer_db_df.size.compressed_size_with_method_gzip.alias('compressed_size'),
                             layer_db_df.size.uncompressed_sum_of_files.alias('uncompressed_size'),
                                 'file_cnt', 'layer_depth.dir_max_depth', F.size('dirs').alias('dir_cnt'))

    layerinfo_df.coalesce(4000).write.save(layer_basic_info3)

# layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR).dropDuplicates('layer_id')
# files = layer_db_df.selectExpr('layer_id', "explode(dirs) As structdirs").selectExpr(
#     'layer_id', 'structdirs.subdir', "explode(structdirs.files) As structdirs_files").selectExpr(
#     'layer_id', 'structdirs.subdir', "structdirs_files.sha256")
# regular_files = files.filter(files.sha256.isNotNull())


def save_specifi_layer_dup_ratio(spark, sc):
    df = spark.read.parquet(layer_dup_ratio).select('layer_id', 'intra_layer_dup_ratio', 'inter_layer_dup_ratio')
    #df.show()
    #df.filter(F.col('intra_layer_dup_ratio') > 0.4).filter(F.col('intra_layer_dup_ratio') <= 0.5).select('layer_id', 'intra_layer_dup_ratio').write.csv('/redundant_layer_analysis/4-5-layer-intra-dup.csv')
    df.filter(F.col('inter_layer_dup_ratio') > 0.9).write.csv('/redundant_layer_analysis/9-10-layer-inter-dup.csv')


def save_layer_redundant_info(spark, sc):

    df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)

    # layer_db_df = spark.read.parquet(LAYER_DB_JSON_DIR).dropDuplicates(['layer_id'])
    # files = layer_db_df.selectExpr('layer_id', "explode(dirs) As structdirs").selectExpr(
    #     'layer_id', "explode(structdirs.files) As structdirs_files").selectExpr(
    #     'layer_id', "structdirs_files.sha256")
    # regular_files = files.filter(files.sha256.isNotNull())

    df = df.select('layer_id', 'digest')

    #df.show(20, False)
    fileinfo = spark.read.parquet(unique_size_cnt_total_sum)
    file_df = fileinfo.select(fileinfo.sha256.alias('digest'), 'avg', 'cnt')
    #file_df.show(20, False)
    lf_info = df.join(file_df, ['digest'], 'inner')

    lf_uniq = lf_info.dropDuplicates(['layer_id', 'digest'])

    uniq_size = lf_uniq.groupby('layer_id').agg(F.sum('avg').alias('sum_files_dropduplicates'),
                                                F.size(F.collect_list('avg')).alias(
                                                                  'cnt_files_dropduplicates'))

    size_df = lf_info.groupby('layer_id').agg(F.sum('avg').alias('sum_files'),
                                              F.size(F.collect_list('avg')).alias(
                                                  'cnt_files')
                                              )

    new_df = uniq_size.join(size_df, ['layer_id'], 'inner')

    shared_df = lf_info.filter('cnt > 1')
    shared_df_layer = shared_df.groupby('layer_id').agg(F.sum('avg').alias('sum_shared_files'),
                                                        F.size(F.collect_list('avg')).alias(
                                                            'cnt_shared_files')
                                                        )

    newer_df = new_df.join(shared_df_layer, ['layer_id'], 'inner')

    new = newer_df.withColumn('intra_layer_dup_ratio',
                              (F.col('sum_files') - F.col('sum_files_dropduplicates')) / F.col('sum_files'))

    new = new.withColumn('inter_layer_dup_ratio', (F.col('sum_shared_files') / F.col('sum_files')))

    new = new.withColumn('intra_layer_dup_ratio_cnt',
                         (F.col('cnt_files') - F.col('cnt_files_dropduplicates')) / F.col('cnt_files'))
    new = new.withColumn('inter_layer_dup_ratio_cnt', (F.col('cnt_shared_files') / F.col('cnt_files')))

    # new = newer_df.withColumn('dropduplicates_ratio_logical', 1-(F.col('sum_files_dropduplicates')/F.col('sum_files')))
    # new = new.withColumn('shared_ratio_logical', (F.col('sum_shared_files')/F.col('sum_files')))
    new.write.save(layer_dup_ratio)


def save_layer_file_cnt(spark, sc):
    df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    new_df = df.groupby('layer_id').agg(F.size(F.collect_list('digest')))
    new_df.write.csv(layer_file_cnt)


def find_file_digest_in_layer(spark, sc):
    #df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    df = spark.read.parquet(layer_db_absfilename3)#LAYER_DB_JSON_DIR)
    #df.show()
    df.filter(df.layer_id == 'sha256:00005e32ef1aa4bde012c8f1f2dc55c3699c8dd8ec3a08326f2b2a0887e9b60b').selectExpr('file_cnt', "explode(dirs) As structdirs").selectExpr('structdirs.subdir', 'structdirs.file_cnt',
     "explode(structdirs.files) As structdirs_files").selectExpr('subdir', 'file_cnt', "structdirs_files.*").show(100, False) #000269aa093202d4e2035086b5e6ab68af8ad8f5b464b49e69478d983ca989db').show()
    


"""save uniqe file digests"""
def save_unique_file_layer_mapping(spark, sc):
    df = spark.read.parquet(LAYER_FILE_MAPPING_DIR)
    new_df = df.dropDuplicates(['digest'])
    #df.show()
    # func = F.udf(get_items, StringType())
    # new_df = df.groupby('digest').agg(func(F.collect_list('filename')).alias('filename'), func(F.collect_list('layer_id')).alias('layer_id'))
    new_df = new_df.select('layer_id','filename')
    new_df.write.csv(unique_file_layer_mapping)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
