
export TIMEFORMAT=%R
export testdir="/home/nannan/testdir/"
#export testdir="/root/nannan/sdb4/testdir/"
testlayerslstfname=$1 #"/root/nannan/testlayers.lst"

export clientaddr="192.168.0.173"
export clientport=1234

#export elapsed_decomprs=0
#export elapsed_unpack=0
#
#export elapsed_pack=0
#export elapsed_comprs=0
#export elapsed_ncat=0
#
#export uncompressed_archival_size=0
#export compressed_archival_size=0

#=======================================
cp_file () {
   cp $1 $2
}

clear_dir () {
    rm -rf "$1"
}

mk_dir () {
    mkdir -pv $1
}

export -f cp_file
export -f clear_dir
export -f mk_dir

#========================================
pack () { # src, dest
	(time tar -cf $2 $1) &> tmp_tar
	elapsed_pack=$(cat tmp_tar | tail -1)
	#return "$elapsed_pack"
}

unpack () {
	(time tar -pxf $1 -C $2) &> tmp_unpack
	elapsed_unpack=$(cat tmp_unpack | tail -1)
	
}

decompress () {
	(time gunzip -c $1 > $2) 2> tmp_decomprs
	elapsed_decomprs=$(cat tmp_decomprs | tail -1)
	
}

compress () {
	echo "compress lz4: $1"
	#lz4 -qf $1
	(time lz4 -qf $1 > "$1.lz4") 2> tmp_comprs
	elapsed_comprs=$(cat tmp_comprs | tail -1)
	echo "done compress"
	
}


decompress_lz4 () {
	echo "decompress lz: $1"
        (time lz4 -qdf $1 > "$2.new.tar") 2> tmp_decomprs
        elapsed_decomprs=$(cat tmp_decomprs | tail -1)
        
}


export -f pack
export -f unpack
export -f decompress_lz4
export -f compress
export -f decompress
#======================================

export outputfname=$(basename $testlayerslstfname)

echo "$outputfname-results.txt"

process_layers () {
	echo $1
	compressed_archival_size=$(stat -c%s "$1")
    	echo "size of $1 = $compressed_archival_size bytes."

	layer_filename=$(basename $1)
	echo "${layer_filename}"
	extracting_dir=$testdir
	layer_dir="$extracting_dir${layer_filename}/"
	
	mk_dir $layer_dir

	cp_layer_tarball_name="$layer_dir${layer_filename}-cp.tar.gz"
	archival_file_name="$layer_dir${layer_filename}-archival.tar"
	unpack_dir="$layer_dir${layer_filename}-unpack_dir"

	mk_dir $unpack_dir

	new_archival_fname="$layer_dir${layer_filename}-new-archival.tar"
	new_comprs_fname="$layer_dir${layer_filename}-new-archival.tar.lz4"

    	cp_file $1 $cp_layer_tarball_name	
	decompress $cp_layer_tarball_name $archival_file_name

	#uncompressed_archival_size=$(stat -c%s "$new_archival_fname")

	unpack $archival_file_name $unpack_dir
	pack $unpack_dir $new_archival_fname

	uncompressed_archival_size=$(stat -c%s "$new_archival_fname")

	compress $new_archival_fname

        compressed_archival_size=$(stat -c%s "$new_comprs_fname")
        echo "size of $1 = $compressed_archival_size bytes."

	decompress_lz4 $new_comprs_fname $new_archival_fname

	(time ncat -w3 $clientaddr $clientport < $new_comprs_fname ) &> tmp_ncat
	elapsed_ncat=$(cat tmp_ncat | tail -1)

	clear_dir $layer_dir

	echo "$elapsed_pack, $elapsed_unpack, $elapsed_decomprs, $elapsed_comprs, $uncompressed_archival_size, $compressed_archival_size, $elapsed_ncat"
	echo -e "$elapsed_pack, $elapsed_unpack, $elapsed_decomprs, $elapsed_comprs, $uncompressed_archival_size, $compressed_archival_size, $elapsed_ncat" >> "$outputfname-results.txt"
	#return $elapsed_pack, $elapsed_unpack, $elapsed_decomprs, $elapsed_comprs, $uncompressed_archival_size, $compressed_archival_size, $elapsed_ncat
}

export -f process_layers

cat $testlayerslstfname | parallel -j 1 process_layers {}

echo "cnt: $(wc "$outputfname-results.txt")"

average_pack_sum=$(awk '{ sum += $1 } END { if (NR > 0) print sum / NR }' "$outputfname-results.txt") #$(echo "scale=3;$elapsed_pack_sum / $cnt" | bc)
average_unpack_sum=$(awk '{ sum += $2 } END { if (NR > 0) print sum / NR }' "$outputfname-results.txt") #$(echo "scale=3;$elapsed_unpack_sum / $cnt" | bc)
average_decomprs_sum=$(awk '{ sum += $3 } END { if (NR > 0) print sum / NR }' "$outputfname-results.txt")  #$(echo "scale=3;$elapsed_decomprs_sum / $cnt" | bc)
average_comprs_sum=$(awk '{ sum += $4 } END { if (NR > 0) print sum / NR }' "$outputfname-results.txt") #$(echo "scale=3;$elapsed_comprs_sum / $cnt" | bc)
uncompressed_archival_size_sum_avg=$(awk '{ sum += $5 } END { if (NR > 0) print sum / NR }' "$outputfname-results.txt") #$(echo "scale=3;$uncompressed_archival_size_sum / $cnt" | bc)
compressed_archival_s_sum_avg=$(awk '{ sum += $6 } END { if (NR > 0) print sum / NR }' "$outputfname-results.txt") #$(echo "scale=3;$compressed_archival_s_sum / $cnt" | bc)
elapsed_ncat_sum_avg=$(awk '{ sum += $7 } END { if (NR > 0) print sum / NR }' "$outputfname-results.txt") #$(echo "scale=3;$elapsed_ncat_sum / $cnt" | bc)

echo "average_pack_sum: $average_pack_sum s"
echo "average_unpack_sum: $average_unpack_sum s"
echo "average_decomprs_sum: $average_decomprs_sum s"
echo "average_comprs_sum: $average_comprs_sum s"
echo "uncompressed_archival_size_sum_avg: $uncompressed_archival_size_sum_avg B"
echo "compressed_archival_s_sum_avg: $compressed_archival_s_sum_avg B"
echo "elapsed_ncat_sum_avg: $elapsed_ncat_sum_avg s"

#process_one () {
#
#	line=$1
#	process_layers "$line"
#
#	#cnt=$(($cnt + 1))
#	#elapsed_pack_sum=$(echo "scale=3;$elapsed_pack_sum + $elapsed_pack" | bc)
#	#elapsed_unpack_sum=$(echo "scale=3;$elapsed_unpack_sum + $elapsed_unpack" | bc)
#	#elapsed_decomprs_sum=$(echo "scale=3;$elapsed_decomprs_sum + $elapsed_decomprs" | bc)
#	#elapsed_comprs_sum=$(echo "scale=3;$elapsed_comprs_sum + $elapsed_comprs" | bc)
#	#uncompressed_archival_size_sum=$(echo "$uncompressed_archival_size_sum + $uncompressed_archival_size" | bc)
#	#compressed_archival_s_sum=$(echo "$compressed_archival_s_sum + $compressed_archival_size" | bc)
#	#elapsed_ncat_sum=$(echo "scale=3;$elapsed_ncat_sum + $elapsed_ncat" | bc)
#
#	echo -e "$elapsed_pack, $elapsed_unpack, $elapsed_decomprs, $elapsed_comprs, $uncompressed_archival_size, $compressed_archival_size, $elapsed_ncat" >> "$outputfname-results.txt"
#
#}

