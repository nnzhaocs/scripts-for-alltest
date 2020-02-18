
dir=$(date +%Y%m%d_%H%M%S)
echo $dir
mkdir -p /home/nannan/testing/results/$dir
sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i "sshpass -p 'kevin123' scp /home/nannan/scripts-for-alltest/benchmarkdockercontainer/iobenchmark/*res.txt   root@192.168.0.200:/home/nannan/testing/results/$dir"

cat /home/nannan/testing/results/$dir/* > res.lst
cat res.lst 

devicemapper='thor6'
btrfs='thor16'
overlay2='thor13'
zfs='thor14'

driver=''

echo -e "rwtp-drivertype-testmode-writemode \t IPOS \t BW \t clat-Unit \t MAX \t AVG." >> "${dir}.vals.lst"

#fname=$(basename $f)
#tmp=$(cat res.lst | awk -F'.' '{print $1}')

#if [[ "$tmp" == *"$devicemapper"* ]]; then
#	echo "It's devicemapper"
#	driver='devicemapper'
#elif [[ "$tmp" == *"$btrfs"* ]]; then
#	echo "It's btrfs"
#	driver='btrfs'
#elif [[ "$tmp" == *"$overlay2"* ]]; then
#	echo "It's overlay2"
#	driver='overlay2'
#elif [[ "$tmp" == *"$zfs"* ]]; then
#	echo "It's zfs"
#	driver='zfs'
#fi
#tp=$(cat res.lst | awk -F'[._/:=,()]' '{print $1-$2-$3 "\t" $7}')

tp=$(cat res.lst | awk -F'[._/:=,()]' '{print $7"-"$1"-"$2"-"$3}')
iopsclat=$(cat res.lst | awk -F'[:=,()]' '{print $3 "\t" $5 "\t" $10 "\t" $15 "\t" $17}')

#paste <(echo "$tmp") <(echo "$tp") <(echo "$iopsclat") --delimiters '\t'
paste <(echo "$tp") <(echo "$iopsclat") --delimiters '\t' >> "${dir}.vals.lst"

echo "${dir}.vals.lst"
cat "${dir}.vals.lst" | column -t
