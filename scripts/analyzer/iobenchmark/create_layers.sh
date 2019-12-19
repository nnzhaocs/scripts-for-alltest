
#docker run --name test --rm nnzhaocs/fiodifflayertest sleep 300 &
#
#docker exec -ti test fio --name=directory --nrfiles=100 --size=1600k --filename_format='/$jobnum/$jobnum/$filenum.f' --bs=16k --direct=1 --rw=write --allow_file_create=0 --numjobs=32 --runtime=300 --group_reporting
#
#docker commit test nnzhaocs/fiodifflayertest-1
#
#
#docker stop $(docker ps -a -q)
#docker rm $(docker ps -a -q)

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



