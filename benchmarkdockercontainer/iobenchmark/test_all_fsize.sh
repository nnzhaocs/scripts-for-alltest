
#$nrfs $filesize $rwtp $blksize $nrjobs $testmode

./cleanup.sh
./fio-test-input.sh 2600 "128k" "randread" 2 3 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh 

./cleanup.sh
./fio-test-input.sh 2600 "128k" "randread" 2 3 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 2600 "64k" "randread" 2 3 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 2600 "64k" "randread" 2 3 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh


./cleanup.sh
./fio-test-input.sh 2600 "32k" "randread" 2 3 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 2600 "32k" "randread" 2 3 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh


./cleanup.sh
./fio-test-input.sh 2600 "16k" "randread" 2 3 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 2600 "16k" "randread" 2 3 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh


./cleanup.sh
./fio-test-input.sh 2600 "8k" "randread" 2 3 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 2600 "8k" "randread" 2 3 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh



./cleanup.sh
./fio-test-input.sh 2600 "4k" "randread" 2 3 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 2600 "4k" "randread" 2 3 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh















































