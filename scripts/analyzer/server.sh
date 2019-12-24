


# this is registry and it sends layers to client. no get request needed!

# first randomly select few files and pack 1 layers

# send layer to client

hulk2file="/home/nannan/sampled.lst"
testdir="/home/nannan/testdir/"
layer="${testdir}testlayer"


inputfile=$hulk2file
outputfile="tmpsample.lst"

samplesize=$1
clientaddr="192.168.0.173"
clientport=1234

./random_select_and_packing.py $inputfile $samplesize $outputfile 


time mkdir -p $layer

time cat $outputfile parallel -j 16 cp {} $layer

time tar -zcf "$layer.tar.gz" $layer

time nc -w3 $clientaddr $clientport < "$layer.tar.gz"

stat --printf="%s" "$layer.tar.gz"

rm -f "$layer.tar.gz"

#time dd if=/dev/zero bs=1024K count=512 | nc -w3 192.168.0.170 1234
