
##dependencies
#yum install libssl-devel
#
#
#https://github.com/nnzhaocs/DeathStarBench/tree/master/socialNetwork
#
#python3 scripts/init_social_graph.py
#
## install norch
#http://torch.ch/docs/getting-started.html
#
#git clone https://github.com/torch/distro.git ~/torch --recursive
#cd ~/torch; bash install-deps;
#./install.sh
#source ~/.bashrc
ipaddress="http://10.101.89.190:8080"


cd /root/DeathStarBench/socialNetwork/wrk2
#./wrk -D exp -t <num-threads> -c <num-conns> -d <duration> -L -s ./scripts/social-network/compose-post.lua http://localhost:8080/wrk2-api/post/compose -R <reqs-per-sec>
 ./wrk -D exp -t $1 -c $2 -d $3 -L -s ./scripts/social-network/read-user-timeline.lua "${ipaddress}/wrk2-api/post/compose" -R $4





