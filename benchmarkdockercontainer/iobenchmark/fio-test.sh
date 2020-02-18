#================ distributions ================
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
rwtp="randread"
blksize=2
blocksize="${blksize}k"
nrjobs=3
totalruntime=600
#testmode="rawfs"
testmode=$1 #"container" # or rawfs or container
ioeng="libaio"

tsize=$(echo "$blksize * $nrfs * 3" | bc)
testsize="${tsize}k" # 4k * 90 * 3 # testsize = (blocksize * nrfiles) * 3

#====================== commands ==========================================

sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'df -hT'
sleep 1
sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'cd /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/; git pull'
sleep 1

date

if [ "$testmode" == "container" ] ; then
	cmd=$(printf "cd /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/; ./cow_test_layers.sh %d %s %s %d %d %s %s &> container-log" $nrfs $filesize $rwtp $blksize $nrjobs $testmode $ioeng)
	echo $cmd

	sshpass -p 'kevin123' pssh -h thors.lst -l root -t 90 -A -i $cmd
	sleep 10

	sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'cd /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/; tail -n 50 container-log'
	
	sleep 10
	sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'docker ps -a'
	
else

	cmd=$(printf "cd /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/; ./cow_test_layers.sh %d %s %s %d %d %s %s &> rawfs-log" $nrfs $filesize $rwtp $blksize $nrjobs $testmode $ioeng)
	echo $cmd
	
	sshpass -p 'kevin123' pssh -h thors.lst -l root -t 90 -A -i $cmd
	sleep 10

	sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'cd /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/; tail -n 50 rawfs-log'

	sleep 10	
	sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'ps aux|grep fio'
fi

























#====================================================================================
#nrfs=2000
#filesize=128
#testsize=$(echo "$nrfs * $filesize"|bc)
#rwtp="read"
#nrjobs=2
#blocksize="4k"
#
#totalruntime=600
##totaliosize='2g'
#
#echo "docker run --rm ljishen/fio --size=$testsize'k' --directory=/ --name=12 --nrfiles=$nrfs --direct=1 -bs=$blocksize --rw=$rwtp --group_report --numjobs=$nrjobs"
#
##docker run --rm ljishen/fio --size=$testsize"k" --directory=/ --name=12 --nrfiles=$nrfs --direct=1 -bs=$blocksize --rw=$rwtp --group_report --numjobs=$nrjobs
#
#read varname
#echo "input: $varname"
#
##fio --size=$testsize"k" --directory=/home --name=12 --nrfiles=$nrfs --direct=1 -bs=$blocksize --rw=$rwtp --group_report --numjobs=$nrjobs
#
#
#fio --ioengine=rbd --size=$testsize"k" --pool=rbd --rbdname=rbd --clientname=admin --clustername=ceph --name=12 --nrfiles=$nrfs --direct=1 -bs=$blocksize --rw=$rwtp --group_report --numjobs=$nrjobs
#
#
#
#
#



