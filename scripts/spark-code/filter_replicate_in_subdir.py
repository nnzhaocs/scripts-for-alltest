# -*- coding: utf-8 -*-
from analysis_library import *
from collections import defaultdict

HDFS_DIR = 'hdfs://hulk0:8020/'

# layer_file_mapping1 = os.path.join('layer_file_mapping/', 'layer_file_mapping_nannan_2tb_hdd.parquet')
# layer_file_mapping2 = os.path.join('layer_file_mapping/', 'layer_file_mapping_2tb_hdd.parquet')
# layer_file_mapping3 = os.path.join('layer_file_mapping/', 'layer_file_mapping_1gb_layer.parquet')

LAYER_FILE_MAPPING_DROPSUB = os.path.join(HDFS_DIR, "layer_file_mapping_drop_subfiles/")
layer_file_mapping_dropsub1 = os.path.join(LAYER_FILE_MAPPING_DROPSUB, 'layer_file_mapping_nannan_2tb_hdd.parquet')
layer_file_mapping_dropsub2 = os.path.join(LAYER_FILE_MAPPING_DROPSUB, 'layer_file_mapping_2tb_hdd.parquet')
layer_file_mapping_dropsub3 = os.path.join(LAYER_FILE_MAPPING_DROPSUB, 'layer_file_mapping_1gb_layer.parquet')

layer_mapping_file = layer_file_mapping1

layer_file_mapping_dropsub = layer_file_mapping_dropsub1

all_json_absfilename1 = os.path.join(LOCAL_DIR, 'nannan_2tb_json.lst')
all_json_absfilename2 = os.path.join(LOCAL_DIR, '2tb_hdd_json.lst')#)#'all_json_absfilename.lst')
# all_json_absfilename3 = os.path.join(LOCAL_DIR, '2tb_hdd_json.lst')#/var/1gb_json.json

output_absfilename1 = os.path.join(VAR_DIR, 'layer_file_mapping_nannan_2tb_hdd_uniq.parquet')
output_absfilename2 = os.path.join(VAR_DIR, 'layer_file_mapping_2tb_hdd_uniq.parquet')
output_absfilename3 = os.path.join(VAR_DIR, 'layer_file_mapping_1gb_layer_uniq.parquet')

list_elem_num = 10000

def main():

    sc, spark = init_spark_cluster()
    # drop_nosub_files(spark, sc)

    absfilename_list = spark.read.text(all_json_absfilename1).collect()
    absfilenames = [str(i.value) for i in absfilename_list]

    extract_file_digests(absfilenames, spark)


def combine_new_name(digest, filename):
    return filename+'-123sha256321-'+digest

def find_root_dir(absfilenames):
    # find = False
    for filename in absfilenames:
        dirname = os.path.dirname(filename)
        if dirname == '/':
            return True
    return False


def check_subdirs(dirnames, absfilenames):
    has_subdir = False
    for dirname in dirnames:
        has_subdir = False
        for filename in absfilenames:
            dirdirname = os.path.dirname(os.path.dirname(filename))
            if dirdirname == dirname:
                has_subdir = True

    return has_subdir


def filter_nosubdirs(absfilenames):
    dup_root_filenames = []

    if not find_root_dir(absfilenames):
        return []

    file_dirs = defaultdict(list)
    root_files = []
    for filename in absfilenames:
        dirname = os.path.dirname(filename)
        if dirname == '/':
            root_files.append(os.path.basename(filename))
        else:
            file_dirs[os.path.basename(filename)].append(os.path.dirname(filename))

    filenames = file_dirs.keys()
    for root_file in root_files:
        if root_file in filenames:
            if not check_subdirs(file_dirs[root_file], absfilenames):
                dup_root_filenames.append(os.path.join('/', root_file))

    return dup_root_filenames


def filter_root_files(filename):
    dirname = os.path.dirname(filename)
    if dirname == '/':
        return True
    return False


def extract_dir_file_digests(layer_id, dirs):

    allfile_lst = []

    for dir in dirs:
        dirname = dir['subdir']
        if not dir['subdir']:
            dirname = '/'
        if dir['files']:
            for f in dir['files']:
                filename = f['filename']
                if f['sha256']:
                    newfname = combine_new_name(f['sha256'], os.path.join(dirname, filename))
                    allfile_lst.append(newfname)

    dup_root_file_names = filter_nosubdirs(allfile_lst)

    duproot_flst = []

    for filename in dup_root_file_names:
        # f_info = {}
        f_info = {
            # 'layer_filename': os.path.basename(layer_filename),
            'layer_id':layer_id,
            'filename': filename.split('-123sha256321-')[0],
            'digest': filename.split('-123sha256321-')[1]
        }
        duproot_flst.append(f_info)#f['sha256'])

    """file_cnt = len(set(file_lst))
    intersect_cnt = len(set(file_lst)&set(duplicate_files))
    return file_cnt*(0.1)/intersect_cnt#"""
    return duproot_flst


def extract_file_digests(absfilename_list, spark):

    extractfiles = udf(extract_dir_file_digests, ArrayType(StructType([
                    # StructField('layer_filename', StringType(), True),
                    StructField('layer_id', StringType(), True),
                    StructField('filename', StringType(), True),
                    StructField('digest', StringType(), True)])))
    # extract_filename = udf(lambda s: os.path.basename(s))
    ##extract_filename("input_file_name()").alias("layer_filename"),

    sublists = split_list(absfilename_list)

    for sublist in sublists:
        """=======================================> modify here"""
        #layer_db_df = spark.read.json(sublist)
        layer_db_df = spark.read.json('/var/1gb_json.json', multiLine=True)#sublist)
        # layer_db_df.printSchema() #"input_file_name()"

        df = layer_db_df.select(extractfiles(layer_db_df.layer_id, layer_db_df.dirs).alias('files'))
        df_f = df.select(F.explode('files').alias('fs'))

        # new_df = df_f.withColumn('layer_filename', col('fs.layer_filename'))
        new_df = df_f.withColumn('layer_id', F.col('fs.layer_id'))
        new_df = new_df.withColumn('filename', F.col('fs.filename'))
        new_df = new_df.withColumn('digest', F.col('fs.digest'))

        new_df = new_df.select('layer_filename', 'layer_id', 'filename', 'digest')
        new_df.coalesce(400).write.save(output_absfilename3, mode='append')
        break
        print("===========================>finished one sublist!!!!!!!!!")


def split_list(datalist):

    sublists = [datalist[x:x+list_elem_num] for x in range(0, len(datalist), list_elem_num)]
    return sublists


def drop_nosub_files(spark, sc):
    df = spark.read.parquet(layer_mapping_file)
    filterrootfile = F.udf(filter_root_files)
    df = df.withColumn('rootfile', filterrootfile('filename'))
    print(df.filter(F.col('rootfile') == True).count())
    return
    combin_func = F.udf(combine_new_name)

    df = df.withColumn('new_filename', combin_func('digest', 'filename'))
    df.show()

    dirs = df.groupby('layer_id').agg(F.collect_list('new_filename').alias('new_filenames'))

    filternosubdirs = F.udf(filter_nosubdirs)#, ArrayType(StringType()))

    dup_root_fs = dirs.withColumn('dup_root_files', filternosubdirs('new_filenames'))

    new_df = dup_root_fs.select(F.sum('dup_root_files'))#F.explode('dup_root_files').alias('dup_root_files'))
    new_df.show(20, False)
    #new_df.coalesce(4000).write.save(layer_file_mapping_dropsub)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
