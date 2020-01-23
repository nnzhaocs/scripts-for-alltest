
#!/bin/bash

#inputfilenames=("hulk4_layers_less_50m.lst" "hulk4_layers_less_1g.lst" "hulk4_layers_less_2g.lst" "hulk4_layers_bigger_2g.lst")
inputfilenames=("hulk1_sampled_1000.lst")

src_dir="/home/nannan/dockerimages/layers/"
dest_dir="${src_dir}tmp_dir/"

#=======================================
cp_file () {
    #cmd=$(printf "cp %s %s" $1 $2)
    #print $cmd
    cp $1 $2
}

clear_dir () {
    rm -rf "$1"
}

mk_dir () {
    mkdir -pv $1
}
#========================================
elapsed_pack=0
elapsed_unpack=0
elapsed_decompress=0
elapsed_compress=0

cpusage_pack=0
cpusage_unpack=0
cpusage_decompress=0
cpusage_compress=0

memusage_pack=0
memusage_unpack=0
memusage_decompress=0
memusage_compress=0

uncompressed_archival_size=0
compressed_archival_size=0
#==========================================

#pack () { # src, dest
#    perf stat -d -o tmp tar -cf $2 $1
#    results=$(cat tmp |grep "seconds time elapsed\|CPUs utilized")
#    cpusage_pack=$(echo $results|awk -F'CPUs utilized' '{print $1}' |awk -F'#' '{print $2}')
#    elapsed_pack=$(echo $results|awk -F'seconds time elapsed' '{print $1}' |awk -F'utilized' '{print $2}')
#
#}
#
#unpack () {
#    perf stat -d -o tmp tar -pxf $1 -C $2
#    results=$(cat tmp |grep "seconds time elapsed\|CPUs utilized")
#    cpusage_unpack=$(echo $results|awk -F'CPUs utilized' '{print $1}' |awk -F'#' '{print $2}')
#    elapsed_unpack=$(echo $results|awk -F'seconds time elapsed' '{print $1}' |awk -F'utilized' '{print $2}')
#
#}
#
#decompress () {
#    perf stat -d -o tmp gunzip -c $1 > $2
#    results=$(cat tmp |grep "seconds time elapsed\|CPUs utilized")
#    cpusage_decompress=$(echo $results|awk -F'CPUs utilized' '{print $1}' |awk -F'#' '{print $2}')
#    elapsed_decompress=$(echo $results|awk -F'seconds time elapsed' '{print $1}' |awk -F'utilized' '{print $2}')
#}
#
#compress () {
#    perf stat -d -o tmp gzip -9 -f $1
#    results=$(cat tmp |grep "seconds time elapsed\|CPUs utilized")
#    cpusage_compress=$(echo $results|awk -F'CPUs utilized' '{print $1}' |awk -F'#' '{print $2}')
#    elapsed_compress=$(echo $results|awk -F'seconds time elapsed' '{print $1}' |awk -F'utilized' '{print $2}')
#}

#------------------------------------------

pack () { # src, dest
    valgrind --log-file=tmp_1  tar -cf $2 $1
    memusage_pack=$(cat tmp_1|grep 'bytes allocated'|awk -F'bytes allocated' '{print $1}'|awk -F'frees,' '{print $2}'|sed 's/,//g')
}

unpack () {
    valgrind --log-file=tmp_2  tar -pxf $1 -C $2
    memusage_unpack=$(cat tmp_2|grep 'bytes allocated'|awk -F'bytes allocated' '{print $1}'|awk -F'frees,' '{print $2}'|sed 's/,//g')
}

decompress () {
#--keep
    valgrind --log-file=tmp_3  gzip -d -f --keep $1  
    fname=$(echo "${1%.*}")
    mv $fname $2
    memusage_decompress=$(cat tmp_3|grep 'bytes allocated'|awk -F'bytes allocated' '{print $1}'|awk -F'frees,' '{print $2}'|sed 's/,//g')
}

compress () {
    valgrind --log-file=tmp_4  pigz -f $1
    memusage_compress=$(cat tmp_4|grep 'bytes allocated'|awk -F'bytes allocated' '{print $1}'|awk -F'frees,' '{print $2}'|sed 's/,//g')
}

#-------------------------------------------

process_layers () {

    compressed_archival_size=$(stat -c%s "$1")

    echo "size of $1 = $compressed_archival_size bytes."

    layer_filename=$(basename $1)
    extracting_dir=$dest_dir
    layer_dir="$extracting_dir${layer_filename}/"

    mk_dir $layer_dir

    cp_layer_tarball_name="$layer_dir${layer_filename}-cp.tar.gz"
    archival_file_name="$layer_dir${layer_filename}-archival.tar"
    unpack_dir="$layer_dir${layer_filename}-unpack_dir"

    mk_dir $unpack_dir

    new_archival_fname="$layer_dir${layer_filename}-new-archival.tar"

    cp_file $1 $cp_layer_tarball_name
    decompress $cp_layer_tarball_name $archival_file_name

    uncompressed_archival_size=$(stat -c%s "$archival_file_name")

    unpack $archival_file_name $unpack_dir
    pack $unpack_dir $new_archival_fname
    compress $new_archival_fname
    clear_dir $layer_dir
}

#elapsed_pack=0
#elapsed_unpack=0
#elapsed_decompress=0
#elapsed_compress=0
#
#cpusage_pack=0
#cpusage_unpack=0
#cpusage_decompress=0
#cpusage_compress=0

#memusage_pack=0
#memusage_unpack=0
#memusage_decompress=0
#memusage_compress=0

#uncompressed_archival_size=0
#compressed_archival_size=0

   cnt=0
   for lst in ${inputfilenames[@]}; do
       fname="$src_dir$lst"
       while IFS= read -r line
           do
              process_layers "$line"
              cnt=$(($cnt + 1))
              val=$(($cnt % 100))
              if [ $val -eq 0 ]; then
                  echo "cnt: $cnt"
              fi
              #echo -e "$elapsed_pack, $elapsed_unpack, $elapsed_decompress, $elapsed_compress, $cpusage_pack, $cpusage_unpack, $cpusage_decompress, $cpusage_compress, $uncompressed_archival_size, $compressed_archival_size,\n" >> "${lst}results.txt"
	      echo -e "$memusage_pack, $memusage_unpack, $memusage_decompress, $memusage_compress, $uncompressed_archival_size, $compressed_archival_size,\n" >> "${lst}results.txt"
              #break
       done < "$fname"
       break
   done
