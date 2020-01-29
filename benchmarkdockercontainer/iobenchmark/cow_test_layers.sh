
echo "copy on write test"
#================ inital parameters================

nrfss=(90 2600 50000)

filesizes=("1k" "2k" "6k" "18k" "1m") #KB

rwtps=("read" "write" "randread" "randwrite")

blocksizse=("1k" "2k" "4k" "8k" "16k" "32k" "64k" "128k")

layersizes=("3k" "14k" "8000k" "55000k" "900000k")

imgsizes=("190m" "280m" "530m" "800m" "5000m")

#layers=(6 10 12 19 50)

nrjobs=1
totalruntime=600

nrfs=90
filesize="1m"
rwtp="randwrite"
blocksize="1k"

#testsize="2g"
#testsize=$(echo "$nrfs * $filesize"|bc)
# testsize=45m
# 
#=============================================
echo "creating a testing layer"

create_img () {

	docker run --name test_cow --rm nnzhaocs/fio sleep 910 &
	sleep 5
	docker exec -ti test_cow fio --name=cowtest --nrfiles="$nrfs" --filesize="${filesize}" --filename_format='/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --allow_file_create=1 --numjobs=$nrjobs --runtime=$totalruntime --group_reporting
	sleep 5
	docker commit test_cow nnzhaocs/fio_cow-0
	docker exec -ti test_cow ls -hl /
	docker stop $(docker ps -a -q)
}

init_ext4 () {
	fio --name=cowtest --nrfiles="$nrfs" --filesize="${filesize}" --filename_format='/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --allow_file_create=1 --numjobs=$nrjobs --runtime=$totalruntime --group_reporting
}

#==============================================
# overwrite parameters:

testsize="6m" # testsize < (filesize * nrfiles) / 2


#==============================================
echo "test overwrite!"

test_ext4 () {
	sync; echo 1 > /proc/sys/vm/drop_caches

        #testsize="45m"
        #nrfs=45

	fio --name=cowtest --nrfiles="$nrfs" --filename_format='/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --numjobs=$nrjobs --runtime=$totalruntime --group_reporting --ioengine=sync --fdatasync=1 --size=$testsize --file_service_type=random

}

#totallayercnt=2

add_layers () {

	i=0

	while [ $i -lt 50  ] ; do

    		echo "previous layer cnt: $i"

    		sync; echo 1 > /proc/sys/vm/drop_caches

    		#testsize="50m"
		#nrfs=100

    		docker run --name test-$i --rm -m 0 nnzhaocs/fio_cow-$i sleep 250 &

    		sleep 5

    		docker exec -ti test-$i fio --name=cowtest --nrfiles="$nrfs" --filename_format='/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --numjobs=$nrjobs --runtime=$totalruntime --group_reporting --ioengine=sync --fdatasync=1 --size=$testsize --file_service_type=random 

    		layercnt=$(echo "$i + 1" | bc)
    		#layercnt=$i
    		echo "creating layer cnt: $layercnt"
    		sleep 2
    		docker commit test-$i nnzhaocs/fio_cow-$layercnt
    		sleep 10
		# sleep 150
    		((i++))

		docker stop $(docker ps -a -q)
		# docker rm $(docker ps -a -q)
		break
	done
}

# docker rmi -f $(docker ps -a -q)

cowtest_container () {

        create_img
        add_layers
}

cowtest_ext4 () {

        init_ext4
        test_ext4
}

cowtest_ext4 

#cowtest_container

