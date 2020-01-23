
inputfile="/home/nannan/nannan_2tb_hdd/alluniqfilenames.lst" #"/home/nannan/sampled.lst"
outputfile="sampledlayerfiles.lst"

for (( i = 300 ; i < 400 ; i += 1 )) ; do

        echo $i
        #./server.sh $i
	python random_select_and_packing.py $inputfile $i "$outputfile_$i"
done
