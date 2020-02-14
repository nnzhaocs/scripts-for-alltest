
echo "copy on write test"
#================ distributions ================

nrfss=(90 2600 50000)

filesizes=("1k" "2k" "6k" "18k" "1m") #KB

rwtps=("read" "write" "randread" "randwrite")

blocksizse=("1k" "2k" "4k" "8k" "16k" "32k" "64k" "128k")

layersizes=("3k" "14k" "8000k" "55000k" "900000k")

imgsizes=("190m" "280m" "530m" "800m" "5000m")

layers=(6 10 12 19 50)

#================== init parameters =============================

nrfs=90
filesize="1m"

rwtp="randwrite"
blksize=1
blocksize="${blksize}k"

nrjobs=1
totalruntime=600

#testmode="rawfs"
testmode="container" # or rawfs or container

ioeng="libaio"

create_f="${testmode}_create_output.txt"
rewrite_f="${testmode}_rewrite_output.txt"

create_res="${testmode}_create_res.txt"
rewrite_res="${testmode}_rewrite_res.txt"

#testsize="2g"
#testsize=$(echo "$nrfs * $filesize"|bc)
#testsize=45m
 
#=================== overwrite parameters ========================

tsize=$(echo "$blksize * $nrfs * 3" | bc)
testsize="${tsize}k" # 4k * 90 * 3 # testsize = (blocksize * nrfiles) * 3

#==================== cleanup caches & disk cache ======================================

echo "cleanup caches & disk cache"

echo 1 > /proc/sys/vm/drop_caches
hdparm -W 0 /dev/sda

# ==================================================================================

extract_vals () {
	echo "extract result vals"
	echo "$1 => nrfs: $nrfs, filesize: $filesize, rwtp: $rwtp, blksize: $blksize, nrjobs: $nrjobs, $ioeng, overwritesize: $testsize " >> "$2"
	cat "$1" | grep "IOPS=" >> "$2"
	cat "$1" | grep "clat (usec)" >> "$2"
}


#================== create img and files =======================

echo "create img and files"

create_imgwithfiles () {

	echo "====================== create_imgwithfiles ================="

	docker run --name test_cow --rm nnzhaocs/fio sleep 910 &
	sleep 5
	docker exec -ti test_cow fio --name=cowtest --nrfiles="$nrfs" --filesize="${filesize}" --filename_format='/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --allow_file_create=1 --ioengine=$ioeng --numjobs=$nrjobs --runtime=$totalruntime --group_reporting 1> "${create_f}"

	cat "${create_f}"	

	extract_vals "${create_f}" "${create_res}"

	sleep 5
	docker commit test_cow nnzhaocs/fio_cow-0
	docker exec -ti test_cow ls -hl /
	docker stop $(docker ps -a -q)
}

create_files () {

	echo "====================== create files =========================="

        fio --name=cowtest --nrfiles="$nrfs" --filesize="${filesize}" --filename_format='/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --allow_file_create=1 --numjobs=$nrjobs --runtime=$totalruntime --ioengine=$ioeng --group_reporting 1> "${create_f}"

        cat "${create_f}"

        extract_vals "${create_f}" "${create_res}"	

}


rewrite_files () {
	echo "===================== rewrite files ============================"

	fio --name=cowtest --nrfiles="$nrfs" --filename_format='/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --numjobs=$nrjobs --runtime=$totalruntime --group_reporting  --size=$testsize --ioengine=$ioeng --file_service_type=random 1> "${rewrite_f}"

        cat "${rewrite_f}"

        extract_vals "${rewrite_f}" "${rewrite_res}"	

}


rewrite_layer () {

	echo "======================= rewrite to previous layer ======================"

	docker run --name test-0 --rm -m 0 nnzhaocs/fio_cow-0 sleep 250 &

	sleep 5

	docker exec -ti test-0 fio --name=cowtest --nrfiles="$nrfs" --filename_format='/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --numjobs=$nrjobs --runtime=$totalruntime --group_reporting --size=$testsize --ioengine=$ioeng --file_service_type=random 1> ""${rewrite_f}""

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
	echo $testmode
	cowtest_container
else
	echo "$testmode"
	cowtest_rawfs
fi

echo "======================== output results ==================="
cat ${create_res}
cat ${rewrite_res}

