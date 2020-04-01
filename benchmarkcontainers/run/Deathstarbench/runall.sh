
#dependencies
yum install libssl-devel


https://github.com/nnzhaocs/DeathStarBench/tree/master/socialNetwork

kubectl -n social-network get svc nginx-thrift 
and replace cluster ip in init_social_graph.py

python3 scripts/init_social_graph.py

kubectl delete -f k8s-yaml/

# install norch
http://torch.ch/docs/getting-started.html

git clone https://github.com/torch/distro.git ~/torch --recursive
cd ~/torch; bash install-deps;
./install.sh
source ~/.bashrc

cd /root/DeathStarBench/socialNetwork/wrk2
#./wrk -D exp -t <num-threads> -c <num-conns> -d <duration> -L -s ./scripts/social-network/compose-post.lua http://localhost:8080/wrk2-api/post/compose -R <reqs-per-sec>
 ./wrk -D exp -t 20 -c 20 -d 10 -L -s ./scripts/social-network/compose-post.lua http://10.101.89.190:8080/wrk2-api/post/compose -R 100





