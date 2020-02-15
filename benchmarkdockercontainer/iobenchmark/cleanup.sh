
#=== cleanup ====

sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'cd /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/; rm -rf thor*res.txt;'
sleep 1

sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'docker rmi -f $(docker images -q -a)'
sleep 1

sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'docker ps'
sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'docker images'

sleep 2

sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'rm -rf /testing/*'
