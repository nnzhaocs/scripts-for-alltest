
#!/bin/bash

# /etc/ssh/sshd_config
# MaxStartups 100
# service sshd restart


date
inputfilenames=("hulk4_layers_less_50m.lst" "hulk4_layers_less_1g.lst" "hulk4_layers_less_2g.lst" "hulk4_layers_bigger_2g.lst")
src_dir="/home/nannan/dockerimages/layers/"
#dest_dir="${src_dir}tmp_dir/"

cp_file () {
    sshpass -p 'nannan' scp "$1" root@hulk2:/home/nannan/dockerimages/layers/$2/
}

for lst in ${inputfilenames[@]}; do
    fname="${src_dir}$lst"
    echo $fname
    cat $fname | parallel --jobs 16 sshpass -p 'nannan' scp {} root@hulk2:/home/nannan/dockerimages/layers/$lst/
#cp_file {} "$lst"
    break
done
