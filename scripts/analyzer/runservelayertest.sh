
#!/bin/bash
clientaddr="192.168.0.173"
clientport=1234

just_send_layer () {

	for (( i = 350 ; i < 400 ; i += 1 )) ; do

        	echo $i
		layername="layer_$1.tar.gz"

		(time ncat -w3 $clientaddr $clientport < $layername ) &> tmp_ncat
		elapsed_ncat=$(cat tmp_ncat | tail -1)
        
	done


}



for (( i = 300 ; i < 400 ; i += 1 )) ; do

	echo $i
	./server.sh $i
done

#just_send_layer

#./server.sh 2000


