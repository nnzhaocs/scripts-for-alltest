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


def main():

    sc, spark = init_spark_cluster()
    drop_nosub_files(spark, sc)


def combine_new_name(digest, filename):
    return filename+'-sha256-'+digest

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
        return 0 #[]

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

    return len(dup_root_filenames)


def filter_root_files(filename):
    dirname = os.path.dirname(filename)
    if dirname == '/':
        return True
    return False


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
