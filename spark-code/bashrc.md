
# ~/.bashrc

# Set Hadoop-related environment variables
export HADOOP_HOME=/usr/local/hadoop
#export HADOOP_PREFIX=/usr/local/hadoop
export HADOOP_CONF_DIR=/home/nannan/hadoop/conf

# Set JAVA_HOME (we will also configure JAVA_HOME directly for Hadoop later on)
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/

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
export SPARK_HOME=/usr/local/spark
export SPARK_CONF_DIR=/home/nannan/spark/conf

export PATH=$PATH:$SPARK_HOME/bin