
path="/home/nannan/nannan_2tb_hdd/uncompressedlayers/"
filename="/home/nannan/nannan_2tb_hdd/samplelayersnames.lst"
i=0

exec 2<filename

while read line1 <&2 ; do
	echo "$path$line1"
	
	if [ ! "$(ls -A $path$line1)" ]; then echo "empty!"; fi
done
