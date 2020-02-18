
sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'cd /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/; cat rawfs-log'

echo "======> created files on rawfs"
sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'ls "/testing/" | wc'

sleep 10

sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'ps aux|grep fio'
