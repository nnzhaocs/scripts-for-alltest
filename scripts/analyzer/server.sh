# this is registry and it sends layers to client. no get request needed!

# first randomly select few files and pack 1 layers

# send layer to client

export TIMEFORMAT=%R

#hulk2file="/home/nannan/sampled.lst"
testdir="/home/nannan/testdir/"
layer="${testdir}testlayer_$1"
layername="layer_$1.tar.gz"
uniqfilesdir="/home/nannan/sampled-3"

#inputfile=$hulk2file
outputfile="/home/nannan/samplefiles/$1" # this is inputfile :) "tmpsample.lst"

#samplesize=$1
clientaddr="192.168.0.173"
clientport=1234

#python random_select_and_packing.py $inputfile $samplesize $outputfile 

elapsed_mkdir=0
elapsed_cp=0
elapsed_tar=0
elapsed_ncat=0

(time mkdir -p $layer) &> tmp_mkdir
elapsed_mkdir=$(cat tmp_mkdir | tail -1)

(time cat $outputfile | parallel -j 16 cp "$uniqfilesdir/{}" $layer ) &> tmp_cp
elapsed_cp=$(cat tmp_cp | tail -1)

(time tar -zcf $layername $layer ) &> tmp_tar

elapsed_tar=$(cat tmp_tar | tail -1)

(time ncat -w3 $clientaddr $clientport < $layername ) &> tmp_ncat
elapsed_ncat=$(cat tmp_ncat | tail -1)

echo "elapsed_mkdir: $elapsed_mkdir"
echo "elapsed_cp: $elapsed_cp"
echo "elapsed_tar: $elapsed_tar"
echo "elapsed_ncat: $elapsed_ncat"

size=$(stat --printf="%s" $layername)
echo $size

echo -e "$elapsed_mkdir \t $elapsed_cp \t $elapsed_tar \t $elapsed_ncat \t $size" >> mkdircptarncatsize_elapsed.lst

#rm -f $layername

#time dd if=/dev/zero bs=1024K count=512 | nc -w3 192.168.0.170 1234
