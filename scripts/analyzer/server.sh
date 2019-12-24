


# this is registry and it sends layers to client. no get request needed!

# first randomly select few files and pack 1 layers

# send layer to client

hulk2file="/home/nannan/sampled.lst"
testdir="/home/nannan/testdir/"
layer="${testdir}testlayer_$1"
layername="layer_$1.tar.gz"

inputfile=$hulk2file
outputfile="tmpsample.lst"

samplesize=$1
clientaddr="192.168.0.173"
clientport=1234

python random_select_and_packing.py $inputfile $samplesize $outputfile 


time mkdir -p $layer

time cat $outputfile | parallel -j 16 cp {} $layer

time tar -zcf $layername $layer

time ncat -w3 $clientaddr $clientport < $layername

size=$(stat --printf="%s" $layername)
echo $size

#rm -f $layername

#time dd if=/dev/zero bs=1024K count=512 | nc -w3 192.168.0.170 1234
