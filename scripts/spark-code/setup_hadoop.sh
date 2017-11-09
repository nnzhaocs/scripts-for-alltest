
##### download & install hadoop ######
sudo apt-get install default-jdk
wget http://apache.mirrors.hoobly.com/hadoop/common/hadoop-2.8.2/hadoop-2.8.2.tar.gz
tar -zxvf hadoop-2.8.2.tar.gz

##### 
sudo addgroup hadoop
usermod -aG hadoop nannan

mkdir hadoop
sudo chown -R nannan:hadoop hadoop

#### vim $HOME/.bashrc ########

# Set Hadoop-related environment variables
export HADOOP_HOME=/usr/local/hadoop

# Set JAVA_HOME (we will also configure JAVA_HOME directly for Hadoop later on)
export JAVA_HOME=/usr/lib/jvm/java-6-sun

# Some convenient aliases and functions for running Hadoop-related commands
unalias fs &> /dev/null
alias fs="hadoop fs"
unalias hls &> /dev/null
alias hls="fs -ls"

# If you have LZO compression enabled in your Hadoop cluster and
# compress job outputs with LZOP (not covered in this tutorial):
# Conveniently inspect an LZOP compressed file from the command
# line; run via:
#
# $ lzohead /hdfs/path/to/lzop/compressed/file.lzo
#
# Requires installed 'lzop' command.
#
lzohead () {
    hadoop fs -cat $1 | lzop -dc | head -1000 | less
}

# Add Hadoop bin/ directory to PATH
export PATH=$PATH:$HADOOP_HOME/bin
#####-----------------------------------
source ~/.bashrc
#####
ssh-keygen -t rsa -P ""
cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys
vim /etc/hosts
192.168.0.170 hulk0
192.168.0.172 hulk2
192.168.0.151 amaranth1
192.168.0.152 amaranth2
192.168.0.153 amaranth3
192.168.0.154 amaranth4
192.168.0.155 amaranth5
192.168.0.156 amaranth6

ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@hulk0
ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@hulk2
ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@amaranth1
ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@amaranth2
ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@amaranth3
ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@amaranth4
ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@amaranth5
ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@amaranth6

####### vim conf/slaves only master
hulk0
hulk2
amaranth1
amaranth2
amaranth3
amaranth4
amaranth5
amaranth6

####### copying/sharing/distributing config files to reset all nodes - master/slaves #######

sudo rsync -avxP /usr/local/hadoop/ hduser@HadoopSlave1:/usr/local/hadoop/


