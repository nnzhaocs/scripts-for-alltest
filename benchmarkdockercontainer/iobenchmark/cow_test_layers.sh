
echo "copy on write test"

#================== init parameters =============================

nrfs=$1 # 90
filesize=$2 #"1m"

rwtp=$3 #"randwrite"
blksize=$4 #1
blocksize="${blksize}k"

nrjobs=$5 #1
totalruntime=600

#testmode="rawfs"
testmode=$6 #"container" # or rawfs or container

ioeng=$7 #"libaio"

hname=$(hostname)

create_f="${hname}.${testmode}_create_output.txt"
rewrite_f="${hname}.${testmode}_rewrite_output.txt"

res_f="${hname}.res.txt"

create_res="${res_f}"
rewrite_res="${res_f}"

#testsize="2g" #testsize=$(echo "$nrfs * $filesize"|bc) #testsize=45m
 
#=================== overwrite parameters ========================

tsize=$(echo "$blksize * $nrfs * 3" | bc)
testsize="${tsize}k" # 4k * 90 * 3 # testsize = (blocksize * nrfiles) * 3

#==================== cleanup caches & disk cache ======================================

ulimit -n 200000

echo "=====> nrfs: $nrfs, filesize: $filesize, rwtp: $rwtp, blksize: $blksize, nrjobs: $nrjobs, $ioeng, overwritesize: $testsize "

echo "cleanup caches & disk cache"

echo 1 > /proc/sys/vm/drop_caches
hdparm -W 0 /dev/sda

# ==================================================================================

extract_vals () {
	echo "extract result vals"
	echo "$1 \\\\// " | tr -d '\n' >> "$2"
	#echo "$1 => nrfs: $nrfs, filesize: $filesize, rwtp: $rwtp, blksize: $blksize, nrjobs: $nrjobs, $ioeng, overwritesize: $testsize " >> "$2"
	cat "$1" | grep "IOPS=" | tr -d '\n' >> "$2"
	cat "$1" | grep "clat (" >> "$2"
}

#================== create img and files =======================

echo "create img and files"

create_imgwithfiles () {

	echo "====================== create_imgwithfiles ================="

	docker run --name test_cow --rm nnzhaocs/fio sleep 910 &
	sleep 5
	docker exec -i test_cow fio --name=cowtest --nrfiles="$nrfs" --filesize="${filesize}" --filename_format='/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --create_on_open=1 --ioengine=$ioeng --numjobs=$nrjobs --runtime=$totalruntime --group_reporting 1> "${create_f}"

	cat "${create_f}"	

	extract_vals "${create_f}" "${create_res}"

	sleep 5
	docker commit test_cow nnzhaocs/fio_cow-0
	docker exec -i test_cow ls -hl /
	docker stop $(docker ps -a -q)
}

create_files () {

	echo "====================== create files =========================="

        fio --name=cowtest --nrfiles="$nrfs" --filesize="${filesize}" --filename_format='/testing/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --create_on_open=1 --numjobs=$nrjobs --runtime=$totalruntime --ioengine=$ioeng --group_reporting 1> "${create_f}"

	ls "/testing/" | wc
        cat "${create_f}"

        extract_vals "${create_f}" "${create_res}"	

}


rewrite_files () {
	echo "===================== rewrite files ============================"

	fio --name=cowtest --nrfiles="$nrfs" --filename_format='/testing/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --numjobs=$nrjobs --runtime=$totalruntime --group_reporting  --size=$testsize --ioengine=$ioeng --allow_file_create=0 --file_service_type=random 1> "${rewrite_f}"

        cat "${rewrite_f}"

        extract_vals "${rewrite_f}" "${rewrite_res}"	

}

rewrite_layer () {

	echo "======================= rewrite to previous layer ======================"

	docker run --name test-0 --rm -m 0 nnzhaocs/fio_cow-0 sleep 250 &

	sleep 5

	docker exec -i test-0 fio --name=cowtest --nrfiles="$nrfs" --filename_format='/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --numjobs=$nrjobs --runtime=$totalruntime --group_reporting --size=$testsize --ioengine=$ioeng --allow_file_create=0 --file_service_type=random 1> ""${rewrite_f}""

        cat "${rewrite_f}"

        extract_vals "${rewrite_f}" "${rewrite_res}"

	echo "rewriting and creating a layer"
	sleep 2
	docker commit test-0 nnzhaocs/fio_cow-1
	sleep 10

	docker stop $(docker ps -a -q)
	# docker rm $(docker ps -a -q)
}

# docker rmi -f $(docker ps -a -q)

cowtest_container () {

        create_imgwithfiles
	sleep 10
        rewrite_layer
}

cowtest_rawfs () {

        create_files
	sleep 10
        rewrite_files
}


if [ "$testmode" == "container" ] ; then
	echo "$testmode"
	cowtest_container
else
	echo "$testmode"
	cowtest_rawfs
fi

echo "======================== output results ==================="

cat ${res_f}


