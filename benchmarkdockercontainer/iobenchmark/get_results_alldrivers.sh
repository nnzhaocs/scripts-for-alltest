
dir=$(date +%Y%m%d_%H%M%S)
echo $dir
mkdir -p /home/nannan/testing/results/$dir
sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i "sshpass -p 'kevin123' scp /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/*res.txt   root@192.168.0.200:/home/nannan/testing/results/$dir"

cat /home/nannan/testing/results/$dir/* > res.lst
cat res.lst 
