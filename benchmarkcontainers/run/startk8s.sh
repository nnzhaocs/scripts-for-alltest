
sshpass -p 'kevin123' pssh -h allnodes.lst -l root -A -i 'ps aux|grep kube'
#sshpass -p 'kevin123' pssh -h allnodes.lst -l root -A -i 'service docker restart'

#sshpass -p 'kevin123' pssh -h allnodes.lst -l root -A -i -t 60 'docker rmi -f $(docker images -a -q)'
#sleep 1

sshpass -p 'kevin123' pssh -h allnodes.lst -l root -A -i 'swapoff -a'
sleep 1

sudo kubeadm init --apiserver-advertise-address=192.168.0.200 --pod-network-cidr=172.16.0.0/16 
sleep 1

mkdir -p $HOME/.kube
sudo cp /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config
#$KUBECONFIG=$HOME/.kube/config
export KUBECONFIG=$HOME/.kube/config

sleep 1

kubectl apply -f calico.yaml

sleep 1

#===============

#sshpass -p 'kevin123' pssh -h workers.lst -l root -A -i ''

#kubectl apply -f https://docs.projectcalico.org/v3.9/manifests/calicoctl.yaml
#alias calicoctl="kubectl exec -i -n kube-system calicoctl /calicoctl -- "
#kubectl proxy --port=9999 --address='192.168.0.200' --accept-hosts="^*$" &

#sleep 1
#kubectl create serviceaccount -n kube-system tiller
#kubectl get clusterrole cluster-admin -o yaml -n kube-system
#kubectl create clusterrolebinding tiller-cluster-admin --clusterrole=cluster-admin --serviceaccount=kube-system:tiller
#kubectl --namespace kube-system get deploy tiller-deploy -o yaml

