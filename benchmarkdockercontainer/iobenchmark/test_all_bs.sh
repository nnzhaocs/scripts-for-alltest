
#$nrfs $filesize $rwtp $blksize $nrjobs $testmode

./cleanup.sh
./fio-test-input.sh 2600 "128k" "randwrite" 128 3 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh 

./cleanup.sh
./fio-test-input.sh 2600 "128k" "randwrite" 128 3 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 2600 "128k" "randwrite" 64 3 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 2600 "128k" "randwrite" 64 3 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh


./cleanup.sh
./fio-test-input.sh 2600 "128k" "randwrite" 32 3 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 2600 "128k" "randwrite" 32 3 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh


./cleanup.sh
./fio-test-input.sh 2600 "128k" "randwrite" 16 3 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 2600 "128k" "randwrite" 16 3 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh


./cleanup.sh
./fio-test-input.sh 2600 "128k" "randwrite" 8 3 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 2600 "128k" "randwrite" 8 3 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh



./cleanup.sh
./fio-test-input.sh 2600 "128k" "randwrite" 4 3 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 2600 "128k" "randwrite" 4 3 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh















































