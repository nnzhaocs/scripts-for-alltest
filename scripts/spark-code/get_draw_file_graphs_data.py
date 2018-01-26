# -*- coding: utf-8 -*-
#import sys
#sys.path.append('../plotter/')
#from draw_pic import *
from analysis_library import *
import re
# import pandas as pd


draw_type_by_repeat_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_type_by_repeat_cnt.csv')
draw_type_by_total_sum = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_type_by_total_sum.csv')
draw_type1_by_repeat_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_type1_by_repeat_cnt.csv')
draw_type_by_size = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_type_by_size.csv')
draw_type_by_dup_ratio_cap = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_type_by_dup_ratio_cap.csv')
draw_type_by_dup_ratio_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_type_by_dup_ratio_cnt.csv')
capacity_data = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'capacity_data.csv')
draw_size_uniq = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_size_uniq.csv')
draw_size_shared = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_size_shared.csv')
draw_size_whole = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_size_whole.csv')
draw_repeat_cnt = os.path.join(REDUNDANT_FILE_ANALYSIS_DIR, 'draw_repeat_cnt.csv')


def main():
    sc, spark = init_spark_cluster()
    #save_file_type(spark, sc)
    #save_file_type_by_repeat_cnt(spark, sc)
    #save_file_size(spark, sc)
    #save_file_repeat_cnt(spark, sc)
    #calculate_capacity(spark, sc)
    #save_file_type_by_repeat_cnt(spark, sc)
    # save_top_1000_files_layer(spark, sc)
    #save_clustering_file_types(spark, sc)
    save_by_type(spark, sc)


def filter_whole_types(tstrs):
    # ELF and excutable,
    #words = re.split(' |; |, |\"|\" ', tstr)
    if len(tstrs):
        tstr = tstrs[0]
    else:
        return 'non-type'

    words = re.split(' |; |, |\"|\" ', tstr)
    if 'ELF' in words:
        return filter_ELF_types(words)
    elif 'executable' in words:
        return filter_non_ELF_executable_types
    elif 'relocatable' in words:
        return 'nonELF-relocatable'
    elif 'compiled' in words or 'Compiled' in words:
        return filter_compiled(words)
    elif 'library' in words:
        return filter_library(words)
    elif "precompiled" in words:
        return 'gcc-precompiled-header'
    elif 'RPM' in words and 'bin' in words:
        return 'RPM-bin-pak'
    elif 'Debian' in words and 'binary' in words:
        return 'debian-bin-pak'
    elif 'image' in words:
        return filter_image(words)
    elif 'document' in words:
        return filter_doc(words)
    elif 'c' in words or 'C' in words or 'C++' in words or 'c++' in words:
        return 'c-c--source'

    re, find =  filter_database(words)
    if find:
        return re
    else:
        re, find = filter_archival(words)
        if find:
            return re
        else:
            re, find = filter_java(words)
            if find:
                return re
            else:
                return tstr

def filter_java(words):
    lowcase_words = []
    for word in words:
        lowcase_words.append(word.lower())

    if 'java' in lowcase_words and 'keystore' in lowcase_words:
        return 'java-keystore'
    elif 'java' in lowcase_words and 'serialization' in lowcase_words:
        return 'java-serialization'



def filter_archival(words):
    lowcase_words = []
    for word in words:
        lowcase_words.append(word.lower())

    if 'zip' in lowcase_words:
        return 'zip-arch', True
    elif 'gzip' in lowcase_words:
        return 'gzip-arch', True
    elif 'xz' in lowcase_words:
        return 'xz-arch', True
    elif 'bzip2' in lowcase_words:
        return 'bzip2-arch', True
    elif 'tar' in lowcase_words:
        return 'tar-arch', True
    elif 'archive' in lowcase_words:
        return 'other-arch', True
    else:
        return None, False


def filter_database(words):
    lowcase_words = []
    for word in words:
        lowcase_words.append(word.lower())

    if 'berkeley' in lowcase_words and 'db' in lowcase_words:
        return 'berkely-db', True
    elif 'dbase' in lowcase_words:
        return 'dbase-db', True
    elif 'sqlite' in lowcase_words:
        return 'sqlite-db', True
    elif 'clam' in lowcase_words and 'antivirtus' in lowcase_words:
        return 'clam-anti-db', True
    elif 'ndbm' in lowcase_words and 'database' in lowcase_words:
        return 'ndbm-db', True
    elif 'database' in lowcase_words:
        return 'other-db', True
    else:
        return None, False



def filter_doc(words):
    lowcase_words = []
    for word in words:
        lowcase_words.append(word.lower())
    if 'html' in lowcase_words or 'xhtml' in lowcase_words or 'xml' in lowcase_words:
        return 'html-xml-xhtml-doc'
    elif 'latex' in lowcase_words or 'tex' in lowcase_words or 'bibtex' in lowcase_words:
        return 'latex-tex-bib-doc'
    elif 'postscript' in lowcase_words or 'pdf' in lowcase_words:
        return 'ps-pdf-doc'
    elif 'composite' in lowcase_words:
        return 'composite-doc'
    elif 'microsoft' in lowcase_words:
        return 'microsoft-doc'
    elif 'perl' in lowcase_words and 'pod' in lowcase_words:
        return 'perl-pod-doc'
    elif 'exported' in lowcase_words and 'sgml' in lowcase_words:
        return 'exported-sgml-doc'
    elif 'lyx' in lowcase_words:
        return 'lyx-doc'
    elif 'openoffice.org' in lowcase_words:
        return 'openoffice-doc'
    else:
        return 'other-doc'


def filter_image(words):
    if 'JPEG' in words:
        return 'jpeg-image'
    elif 'PNG' in words:
        return 'png-image'
    elif 'SVG' in words:
        return 'svg-image'
    elif 'TIFF' in words:
        return 'tiff-image'
    elif 'FIG' in words:
        return 'fig-image'
    elif 'FITS' in words:
        return 'fits-image'
    elif 'pixmap' in words and 'X' in words:
        return 'x-pixmap-image'
    elif 'VISX' in words:
        return 'visx-image'
    elif 'Photoshop' in words:
        return 'photoshop-image'
    else:
        return 'other-image'


def filter_compiled(words):
    lowcase_words = []
    for word in words:
        lowcase_words.append(word.lower())
    if 'python' in lowcase_words:
        return 'python byte-compiled'
    elif 'java' in lowcase_words:
        return 'compiled-java-class'
    elif 'emacs' in lowcase_words or 'xemacs' in lowcase_words:
        return 'xemacs-emacs-compiled'
    elif 'terminfo' in lowcase_words:
        return 'terminfo-compiled'
    elif 'psi' in lowcase_words:
        return 'psi-compiled'
    else:
        return 'other-compiled'

def filter_library(words):
    lowcase_words = []
    for word in words:
        lowcase_words.append(word.lower())

    if 'libtool' in lowcase_words:
        return 'libtool-lib'
    elif 'ocaml' in lowcase_words:
        return 'ocaml-lib'
    elif 'palm' in lowcase_words:
        return 'palm-lib'
    elif 'mach-o' in lowcase_words:
        return 'mach-o-lib'
    else:
        return 'other-lib'


def filter_non_ELF_executable_types(words):
    #words = re.split(' |; |, |\"|\" ', tstr)
    lowcase_words = []
    for word in words:
        lowcase_words.append(word.lower())
    if 'script' in lowcase_words:
        return filter_script(lowcase_words)
    else:
        return filter_non_script(lowcase_words)


def filter_script(lowcase_word):
    if 'python' in lowcase_word:
        return "python-script"
    elif 'bash' in lowcase_word:
        return 'bash-shell-script'
    elif 'shell' in lowcase_word:
        return 'bash-shell-script'
    elif 'ruby' in lowcase_word:
        return 'ruby-jruby-script'
    elif 'jruby' in lowcase_word:
        return 'ruby-jruby-script'
    elif 'node' in lowcase_word:
        return  'node-script'
    elif 'perl' in lowcase_word:
        return 'perl-script'
    elif 'php' in lowcase_word:
        return 'php-script'
    else:
        return 'other-script'


def filter_non_script(lowcase_word):
    if 'pe32' in lowcase_word or 'pe32+' in lowcase_word:
        return 'PE-PE32-execu'
    elif 'vax'  in lowcase_word and 'coff' in lowcase_word:
        return 'VAX-COFF-execu'
    else:
        return 'other-nonELF-execu'


def filter_ELF_types(words):
    #words = re.split(' |; |, |\"|\" ', tstr)
    if 'relocatable' in words:
        return 'ELF-relocatable'
    elif 'shared' in words:
        return 'ELF-shared'
    elif 'core'  in words:
        return 'ELF-core'
    elif 'processor-specific' in words:
        return 'ELF-processor-specific'
    else:
        return 'ELF-others'


def save_clustering_file_types(spark, sc):
    file_info = spark.read.parquet(unique_file_basic_info).select('sha256', 'type')
    #func0 = F.udf(lambda s: s[0] if len(s) else None)
    #sha_type = file_info.select('sha256', func0('type').alias('type'))
    #sha_type.show()
    
    func_filter_ELF = F.udf(filter_whole_types, StringType())

    #sha_type = sha_type.select('sha256', 'type')
    filter_ELF = file_info.withColumn('ELF_or_not', func_filter_ELF('type'))

    filter_ELF.show()
    ELF_ts = filter_ELF.filter(F.col('ELF_or_not') == 'ELF')
    print("=============> ELF-ts count: %d", ELF_ts.count())
    func_filter_ELF_types = F.udf(filter_EFL_types, StringType())

    ELF_ts = ELF_ts.withColumn('ELF_types', func_filter_ELF_types('type'))
    print("=============> ELF_elf_ts count: %d", ELF_ts.count())

    #file_type.write.csv('/redundant_file_analysis/draw_file_type.csv')

def save_by_type(spark, sc):
    """save by repeat cnt"""
    file_info = spark.read.parquet(unique_file_basic_info).filter(F.size('type') > 0)

    #func = F.udf(lambda s: s[0]#.replace("sticky ", "").replact("posix ", "").replace("setgid ", "").replace("setuid ", "").replace("compiled ", "").split(" ")[0])
    func0 = F.udf(filter_whole_types)
    func1 = F.udf(lambda s: s[0] if len(s) else None)

    file_info_df = file_info.select(func0('file_name').alias('filename'), 'cnt', func0('type').alias('type'), func1('extension').alias('extension'),
                'total_sum', 'sha256', 'avg')
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

    type1 = file_info_df.groupby('type').agg(F.sum('cnt').alias('total_cnt'), F.sum('avg').alias('size_dropdup'),
                                            F.sum('total_sum').alias('sum_size'),
                                            func0(F.collect_set('extension')).alias('extension'),
                                            F.size(F.collect_list('cnt')).alias('uniq_cnt'))
    type1.printSchema()

    type1 = type1.withColumn('avg_size', F.col('sum_size')*1.0/F.col('total_cnt'))
    type1 = type1.withColumn('dup_ratio_cap', (F.col('sum_size') - F.col('size_dropdup'))/F.col('sum_size')*1.0)
    type1 = type1.withColumn('dup_ratio_cnt', (F.col('total_cnt') - F.col('uniq_cnt'))* 1.0 / F.col('total_cnt'))

    sort_type_cnt = type1.sort(type1.total_cnt.desc())
    #sort_type_cnt.show()
    sort_type_cnt.coalesce(400).write.csv('/redundant_file_analysis/draw_type1_by_cnt_med.csv')#draw_type1_by_repeat_cnt)

    #sort_type_size = type1.sort(type1.sum_size.desc())
    #sort_type_size.show()
    #sort_type_size.coalesce(400).write.csv('/redundant_image_analysis/draw_type1_by_cap_med.csv')#draw_type_by_size)
    """
    sort_type_dup_r = type1.sort(type1.dup_ratio_cap.desc())
    #sort_type_dup_r.show()
    sort_type_dup_r.write.csv(draw_type_by_dup_ratio_cap)

    sort_type_dup_r = type1.sort(type1.dup_ratio_cnt.desc())
    #sort_type_dup_r.show()
    sort_type_dup_r.write.csv(draw_type_by_dup_ratio_cnt)
    """


def save_top_1000_files_layer(spark, sc):
    """save by repeat cnt"""
    layer_file_mapping = spark.read.parquet(LAYER_FILE_MAPPING_DIR).dropDuplicates(['layer_id', 'digest'])
    sort_cnt = spark.read.csv(draw_type_by_repeat_cnt).select('_c5')
    # sort_cnt.printSchema()
    # sort_cnt.show()

    sort_cnt = sort_cnt.select(sort_cnt._c5.alias('digest'))
    sort_cap = spark.read.csv(draw_type_by_total_sum).select('_c5')
    sort_cap = sort_cap.select(sort_cap._c5.alias('digest'))

    df = sort_cnt.join(layer_file_mapping, 'digest', 'inner')
    df.write.csv('/redundant_file_analysis/draw_type_by_repeat_cnt_layerdir.csv')
    df = sort_cap.join(layer_file_mapping, 'digest', 'inner')
    df.write.csv('/redundant_file_analysis/draw_type_by_cap_layerdir.csv')


def save_file_type(spark, sc):
    file_info = spark.read.parquet(unique_file_basic_info).select('type')
    func0 = F.udf(lambda s: s[0] if len(s) else None)
    file_type = file_info.select(func0('type').alias('type'))
    file_type.write.csv('/redundant_file_analysis/draw_file_type.csv')

def save_file_type_by_repeat_cnt(spark, sc):
    """save by repeat cnt"""
    file_info = spark.read.parquet(unique_file_basic_info).filter(F.size('type') > 0)

    func = F.udf(lambda s: s[0].lower().replace("sticky ", "").replact("posix ", "").replace("setgid ", "").replace("setuid ", "").replace("compiled ", "").split(" ")[0])
    func0 = F.udf(lambda s: s[0] if len(s) else None)

    file_info_df = file_info.select(func0('file_name').alias('filename'), 'cnt', func('type').alias('type'), func0('extension').alias('extension'),
                'total_sum', 'sha256', 'avg')
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
    
    type1 = file_info_df.groupby('type').agg(F.sum('cnt').alias('total_cnt'), F.sum('avg').alias('size_dropdup'),
                                            F.sum('total_sum').alias('sum_size'),
                                            func0(F.collect_set('extension')).alias('extension'),
                                            F.size(F.collect_list('cnt')).alias('uniq_cnt'))
    type1.printSchema()
    
    type1 = type1.withColumn('avg_size', F.col('sum_size')*1.0/F.col('total_cnt'))
    type1 = type1.withColumn('dup_ratio_cap', (F.col('sum_size') - F.col('size_dropdup'))/F.col('sum_size')*1.0)
    type1 = type1.withColumn('dup_ratio_cnt', (F.col('total_cnt') - F.col('uniq_cnt'))* 1.0 / F.col('total_cnt'))

    sort_type_cnt = type1.sort(type1.total_cnt.desc())
    #sort_type_cnt.show()
    sort_type_cnt.write.csv(draw_type1_by_repeat_cnt)

    sort_type_size = type1.sort(type1.sum_size.desc())
    #sort_type_size.show()
    sort_type_size.write.csv(draw_type_by_size)
    """
    sort_type_dup_r = type1.sort(type1.dup_ratio_cap.desc())
    #sort_type_dup_r.show()
    sort_type_dup_r.write.csv(draw_type_by_dup_ratio_cap)

    sort_type_dup_r = type1.sort(type1.dup_ratio_cnt.desc())
    #sort_type_dup_r.show()
    sort_type_dup_r.write.csv(draw_type_by_dup_ratio_cnt)
    """

def calculate_capacity(spark, sc):
    df = spark.read.parquet(unique_size_cnt_total_sum)
    """
    df_1 = df.filter(df.cnt == 1)
    df_1_size = df_1.select(F.sum('avg').alias('sum_size_1'))
    df_1_size.show()
    print("==============> df_1:%d", df_1.count())
    #df_1_size.show()
    """
    
    df_2 = df.filter(df.cnt > 1)
    """#df_2_dedup.show()
    df_2_dedup_size = df_2.select(F.sum('avg').alias('sum_size_2_remove_redundancy'))
    df_2_dedup_size.show()
    print("==============> df_2:%d", df_2.count())
    """
    #new_df = df_2.unionAll(df_1)
    """
    #df_2_total_size = df.filter(df.cnt > 1)
    #df_2.show()
    df_2_with_dup = df_2.select(F.sum('total_sum').alias('sum_size_2_redundancy'))
    df_2_with_dup_cnt = df_2.select(F.sum('cnt').alias('total_cnt_2'))
    df_2_with_dup.show()
    df_2_with_dup_cnt.show()
    """
    #new_df = new_df.unionAll(df_2)
    
    df_2_dedup_size_total = df.select(F.sum('avg').alias('sum_files_remove_redundancy'))
    df_2_dedup_size_total.show(20, False)
    print("==============> dedup_cnt_total:%d", df.count())
    #df_1.show()
    #new_df = new_df.unionAll(df_1)
    """
    df_dup_size_total = df.select(F.sum('total_sum').alias('sum_files_redundancy'))
    df_dup_size_total.show()
    df_with_dup_cnt_total = df.select(F.sum('cnt').alias('total_cnt'))
    df_with_dup_cnt_total.show()
    """
    #print("=============> ")
    #df.show()
    #df_dup_size_total.coalesce(1).write.csv(capacity_data, mode='append')
    #df_2_dedup_size_total.coalesce(1).write.csv(capacity_data, mode='append')
    #df_2_with_dup.coalesce(1).write.csv(capacity_data, mode='append')
    #df_2_dedup_size.coalesce(1).write.csv(capacity_data, mode='append')
    #df_1_size.coalesce(1).write.csv(capacity_data, mode='append')
    #new_df = df_dup_size_total.unionAll(df_2_dedup_size_total, df_2_with_dup, df_2_dedup_size, df_1_size) 
    #new_df.show(20, False)
    #new_df.write.csv(capacity_data)
    #print("==============> df_dup_size_total=%d, df_2_dedup_size_total=%d, df_2_with_dup=%d, df_2_dedup_size=%d, df_1_size=%d", df.count(), df_2_with_dup.count(), df_2_dedup_size.count(), df_1_size.count())
    

def save_file_size(spark, sc):
    size_df = spark.read.parquet(unique_size_cnt_total_sum)#.select((F.col('avg')/1024.0).alias('avg')) #kb
    size_df.show()
    size_uniq = size_df.filter(size_df.cnt == 1).select('avg')
    size_shared = size_df.filter(size_df.cnt > 1).select('avg')
    size_whole = size_df.select('avg')

    size_uniq = size_uniq.sort('avg')
    size_shared = size_shared.sort('avg')
    size_whole = size_whole.sort('avg')

    size_uniq.coalesce(1).write.csv(draw_size_uniq)
    size_shared.coalesce(1).write.csv(draw_size_shared)
    size_whole.coalesce(1).write.csv(draw_size_whole)


def save_file_repeat_cnt(spark, sc):
    cnt_df  = spark.read.parquet(unique_size_cnt_total_sum).select('cnt')
    cnt = cnt_df.sort(cnt_df.cnt.desc())
    cnt.write.csv(draw_repeat_cnt)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
