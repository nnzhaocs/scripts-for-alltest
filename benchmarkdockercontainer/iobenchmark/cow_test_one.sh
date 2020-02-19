#================parameters================
nrfss=(90 2600 50000)
filesizes=("1k" "2k" "6k" "18k" "1m") #KB
rwtps=("read" "write" "randread" "randwrite")
blocksizse=("1k" "2k" "4k" "8k" "16k" "32k" "64k" "128k")
layersizes=("3k" "14k" "8000k" "55000k" "900000k")
imgsizes=("190m" "280m" "530m" "800m" "5000m")
layers=(6 10 12 19 50)
#================== init parameters =============================

nrfs=2600 #
filesize="128k"
rwtp="randrw"
blksize=2
blocksize="${blksize}k"
nrjobs=3
totalruntime=600
#testmode="rawfs"
testmode="container" # or rawfs or container
ioeng="libaio"

tsizeW1=$(echo "$blksize * $nrfs * 3" | bc)
tsizeW2=$(echo "$blksize * $nrfs * 3 * 2" | bc)

testsize="${tsize}k" # 4k * 90 * 3 # testsize = (blocksize * nrfiles) * 3

hname=$(hostname)

create_f="${hname}.${testmode}_create_output.txt"
rewrite_f="${hname}.${testmode}_rewrite_output.txt"

res_f="${hname}.res.txt"

create_res="${res_f}"
rewrite_res="${res_f}"

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
        cat "$1" | grep "clat ("| tr -d '\n' >> "$2"
	echo "  " >> "$2"
}

#================== create img and files =======================

create_imgwithfiles () {

        echo "====================== create_imgwithfiles ================="

        docker run --name test_cow --rm nnzhaocs/fio sleep 910 &
        sleep 5
        docker exec -i test_cow fio --name=cowtest --nrfiles="$nrfs" --filesize="${filesize}" --filename_format='/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --ioengine=$ioeng --numjobs=$nrjobs --runtime=$totalruntime --group_reporting 1> "${create_f}"

        cat "${create_f}"

        extract_vals "${create_f}" "${create_res}"

        sleep 5
        docker commit test_cow nnzhaocs/fio_cow-0
        docker exec -i test_cow ls -hl /
        docker stop $(docker ps -a -q)
}

layers=12

i=0
pre=2

add_layers () {

##	if [[ "$pre" -eq 1 ]]; then
##        	testsize="${tsizeW2}k"
##                prev=2
##	else
##                testsize="${tsizeW1}k"
##                prev=1
##	fi
#
#	echo "$prev, $testsize"
	
	while [ $i -lt $layers  ] ; do

		echo " ================== previous layer cnt: $i"

	        if [[ "$pre" -eq 1 ]]; then
	                testsize="${tsizeW2}k"
	                 prev=2
	        else
	                 testsize="${tsizeW1}k"
	                 prev=1
	        fi

	        echo "$prev, $testsize"

		docker run --name test-$i --rm nnzhaocs/fio_cow-$i sleep 250 &

		sleep 5

		docker exec -i test-$i fio --name=cowtest --nrfiles="$nrfs" --filename_format='/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --numjobs=$nrjobs --runtime=$totalruntime --group_reporting --size=$testsize --ioengine=$ioeng --file_service_type=random 1> ""${rewrite_f}.$i""

		cat "${rewrite_f}.$i"

        	extract_vals "${rewrite_f}.$i" "${rewrite_res}"
		
		layercnt=$(echo "$i + 1" | bc)

		echo "creating layer cnt: $layercnt"
		sleep 2

		docker commit test-$i nnzhaocs/fio_cow-$layercnt

		docker stop $(docker ps -a -q)

		((i++))
	done
}
# docker stop $(docker ps -a -q)
# docker rm $(docker ps -a -q)


create_imgwithfiles 

sleep 10

add_layers


