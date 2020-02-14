
i=0

while [ $i -lt 10  ] ; do
	echo "$i: write 5g = 64k*81920"
	dd if=/dev/zero of=/home/testfile bs=64k count=81920 oflag=direct
	rm -rf /home/testfile
done
