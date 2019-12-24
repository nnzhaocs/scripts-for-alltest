
#!/bin/bash

for (( i = 100 ; i < 2000 ; i += 50 )) ; do

	echo $i
	./server.sh $i
done

#./server.sh 2000
