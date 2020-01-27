
nrfs=2000
filesize=128
testsize=$(echo "$nrfs * $filesize"|bc)
rwtp="read"
nrjobs=2
blocksize="4k"

totalruntime=600
#totaliosize='2g'

echo "docker run --rm ljishen/fio --size=$testsize'k' --directory=/ --name=12 --nrfiles=$nrfs --direct=1 -bs=$blocksize --rw=$rwtp --group_report --numjobs=$nrjobs"

#docker run --rm ljishen/fio --size=$testsize"k" --directory=/ --name=12 --nrfiles=$nrfs --direct=1 -bs=$blocksize --rw=$rwtp --group_report --numjobs=$nrjobs

read varname
echo "input: $varname"

#fio --size=$testsize"k" --directory=/home --name=12 --nrfiles=$nrfs --direct=1 -bs=$blocksize --rw=$rwtp --group_report --numjobs=$nrjobs


fio --ioengine=rbd --size=$testsize"k" --directory=/home --name=12 --nrfiles=$nrfs --direct=1 -bs=$blocksize --rw=$rwtp --group_report --numjobs=$nrjobs


#echo "docker run --rm ljishen/fio --name=filesize --directory=/ --nrfiles=$nrfiles --size=$testsize  --direct=1 --rw=$rwtp --bs=$blocksize  --numjobs=$nrjobs --group_reporting"


#docker run --rm ljishen/fio --name="filesize" --directory=/ --nrfiles=$nrfs --size=$testsize  --direct=1 --rw=$rwtp --bs=$blocksize  --numjobs=$nrjobs --group_reporting --runtime=$totalruntime 


