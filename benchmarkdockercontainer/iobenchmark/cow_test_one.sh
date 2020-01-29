
echo "copy on write test"
#================parameters================

nrfss=(90 2600 50000)

filesizes=("1k" "2k" "6k" "18k" "1m") #KB

rwtps=("read" "write" "randread" "randwrite")

blocksizse=("1k" "2k" "4k" "8k" "16k" "32k" "64k" "128k")


#testsize=$(echo "$nrfs * $filesize"|bc)
nrjobs=1
totalruntime=600

nrfs=90
filesize="1k"
rwtp="write"
blocksize="1k"

#=============================================

docker run --name test_cow --rm nnzhaocs/fio sleep 310 &

docker exec -ti test_cow fio --name=cowtest --nrfiles="$nrfs" --filesize="${filesize}k" --filename_format='/$jobnum.$filenum.f' --bs=$blocksize --direct=1 --rw=$rwtp --allow_file_create=1 --numjobs=$nrjobs --runtime=$totalruntime --group_reporting

docker commit test_cow nnzhaocs/fio_cow





i=1

while [ $i -lt 30  ] ; do

    echo "previous layer cnt: $i"

    docker run --name test-$i --rm nnzhaocs/fiodifflayertest-$i sleep 310 &
    sleep 5
    docker exec -ti test-$i fio --name=directory --nrfiles=100 --size=1600k --filename_format='/$jobnum/$jobnum/$filenum.f' --bs=16k --direct=1 --rw=randwrite --allow_file_create=0 --numjobs=32 --runtime=300 --group_reporting

    layercnt=$(echo "$i + 1" | bc)
    echo "creating layer cnt: $layercnt"
    sleep 2
    docker commit test-$i nnzhaocs/fiodifflayertest-$layercnt

    ((i++))

# docker stop $(docker ps -a -q)
# docker rm $(docker ps -a -q)


done



