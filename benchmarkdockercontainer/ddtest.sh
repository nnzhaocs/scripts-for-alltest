
i=0

while [ $i -lt 5  ] ; do
	echo "$i: write 5g = 64k*81920"
	dd if=/dev/zero of=/home/testfile bs=1G count=2 oflag=direct
	sleep 10
	rm -rf /home/testfile
	#((i++))
done
