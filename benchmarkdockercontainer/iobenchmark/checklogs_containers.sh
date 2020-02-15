
sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'cd /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/; tail -n 50 container-log'

sleep 10

sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'docker ps -a'
