
#helm reset

rm -rf /home/nannan/.kube
rm -rf /home/nannan/.helm
sshpass -p 'kevin123' pssh -h allnodes.lst -l root -A -i 'kubeadm reset --force'
sshpass -p 'kevin123' pssh -h allnodes.lst -l root -A -i 'iptables -F && iptables -t nat -F && iptables -t mangle -F && iptables -X'

sleep 1
sshpass -p 'kevin123' pssh -h allnodes.lst -l root -A -i -t 60 'docker stop $(docker ps -q -a)'

sleep 1
sshpass -p 'kevin123' pssh -h allnodes.lst -l root -A -i -t 60 'docker rm -f $(docker ps -a -q)'



