

sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'sudo yum remove -y docker docker-common docker-selinux docker-engine docker-ce-cli'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'sudo yum install -y yum-utils device-mapper-persistent-data lvm2'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i ' sudo yum install -y docker-ce-17.06.1.ce'
sshpass -p 'kevin123' pssh -h 8thors -l root -A -i 'usermod -aG docker nannan'
sshpass -p 'kevin123' pssh -h 8thors -l root -A -i 'sudo systemctl enable docker'
