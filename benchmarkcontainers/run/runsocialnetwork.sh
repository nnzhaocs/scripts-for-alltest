
kubectl apply -f ./ 

sshpass -p 'kevin123' pssh -h allnodes.lst -l root -A -i 'git clone https://github.com/nnzhaocs/DeathStarBench.git'
sshpass -p 'kevin123' pssh -h allnodes.lst -l root -A -i 'ls /root/'

kubectl delete -f ./ 
