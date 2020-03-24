
sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -t 60 -i 'sudo yum remove -y docker docker-common docker-selinux docker-engine docker-ce-cli'
sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -t 60 -i 'sudo yum install -y yum-utils device-mapper-persistent-data lvm2'
sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -t 60 -i 'sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo'
sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -t 60 -i ' sudo yum install -y docker-ce-17.06.1.ce'
sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -t 60 -i 'usermod -aG docker nannan'
sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -t 60 -i 'sudo systemctl enable docker'

sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -t 60 -i ' echo "{ \"storage-driver\": \"overlay\" }" > /etc/docker/daemon.json'
#sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -i 'dockerd'

sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -t 60 -i 'pkill -9 dockerd'
sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -t 60 -i 'systemctl reset-failed docker.service'
sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -t 60 -i 'systemctl start docker.service'
