
echo "============= Parameters =================="
#================ distributions ================

nrfss=(90 2600 50000)
filesizes=("1k" "2k" "6k" "18k" "1m") #KB
rwtps=("read" "write" "randread" "randwrite")
blocksizse=("1k" "2k" "4k" "8k" "16k" "32k" "64k" "128k")
layersizes=("3k" "14k" "8000k" "55000k" "900000k")
imgsizes=("190m" "280m" "530m" "800m" "5000m")
layers=(6 10 12 19 50)

#================== init parameters =============================
#==> nrfs=90
#==> filesize="1m"
#==> rwtp="randwrite"
#==> blksize=1
#==> blocksize="${blksize}k"
#==> nrjobs=1
#==> totalruntime=600
#==> #testmode="rawfs"
#==> testmode="container" # or rawfs or container
#==> ioeng="libaio"
#==> 
#==> hname=$(hostname)
#==> create_f="${hname}.${testmode}_create_output.txt"
#==> rewrite_f="${hname}.${testmode}_rewrite_output.txt"
#==> res_f="${hname}.${testmode}_res.txt"
#==> create_res="${res_f}"
#==> rewrite_res="${res_f}"
#=================== overwrite parameters ========================
#==> tsize=$(echo "$blksize * $nrfs * 3" | bc)
#==> testsize="${tsize}k" # 4k * 90 * 3 # testsize = (blocksize * nrfiles) * 3
#===================== print parameters ==========================================
#echo "$1 => nrfs: $nrfs, filesize: $filesize, rwtp: $rwtp, blksize: $blksize, nrjobs: $nrjobs, $ioeng, overwritesize: $testsize "
#======> ./cow_test_layers.sh $nrfs $filesize $rwtp $blksize $nrjobs $testmode $ioeng 
#====================== commands ==========================================

sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'cd /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/; git pull'

date
sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'cd /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/; ./cow_test_layers.sh 90 "1m" "randwrite" 1 1 "container" "libaio" &> container-log '

sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'cd /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/; tail -n 50 container-log'

sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'docker ps -a'

sleep 20

date
sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i ' cd /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/; ./cow_test_layers.sh 90 "1m" "randwrite" 1 1 "rawfs" "libaio" &> rawfs-log '

sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'cd /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/; tail -n 50 rawfs-log'

sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'ps aux|grep fio'


#=== cleanup ====
sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'cd /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/; rm -rf thor*_res.txt;'

























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



