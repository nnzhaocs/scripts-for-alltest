
#$nrfs $filesize $rwtp $blksize $nrjobs $testmode

./cleanup.sh
./fio-test-input.sh 50000 "18k" "randread" 2 1 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh 

./cleanup.sh
./fio-test-input.sh 50000 "18k" "randread" 2 1 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 10000 "18k" "randread" 2 2 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 10000 "18k" "randread" 2 2 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh


./cleanup.sh
./fio-test-input.sh 10000 "18k" "randread" 2 3 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 10000 "18k" "randread" 2 3 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh


./cleanup.sh
./fio-test-input.sh 10000 "18k" "randread" 2 4 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 10000 "18k" "randread" 2 4 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh


./cleanup.sh
./fio-test-input.sh 10000 "18k" "randread" 2 5 "container"
sleep 1500
./checklogs_containers.sh
./get_results_alldrivers.sh

./cleanup.sh
./fio-test-input.sh 10000 "18k" "randread" 2 5 "rawfs"
sleep 1500
./checklogs_rawfs.sh
./get_results_alldrivers.sh



#./cleanup.sh
#./fio-test-input.sh 50000 "18k" "randread" 2 3 "container"
#sleep 1500
#./checklogs_containers.sh
#./get_results_alldrivers.sh
#
#./cleanup.sh
#./fio-test-input.sh 50000 "18k" "randread" 2 3 "rawfs"
#sleep 1500
#./checklogs_rawfs.sh
#./get_results_alldrivers.sh















































