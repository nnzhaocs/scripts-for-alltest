
#microk8s.stop

sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'sudo yum remove -y docker docker-common docker-selinux docker-engine docker-ce-cli'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'sudo yum install -y yum-utils device-mapper-persistent-data lvm2'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i ' sudo yum install -y docker-ce-17.06.1.ce'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'usermod -aG docker nannan'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'sudo systemctl enable docker'

sshpass -p 'kevin123' pssh -h 8thors -l root -A -i ' echo "{ \"storage-driver": "overlay\" }" > /etc/docker/daemon.json'
#sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'dockerd'

sshpass -p 'kevin123' pssh -h 8thors -l root -A -i 'pkill -9 dockerd'
sshpass -p 'kevin123' pssh -h 8thors -l root -A -i 'systemctl reset-failed docker.service'
sshpass -p 'kevin123' pssh -h 8thors -l root -A -i 'systemctl start docker.service'

# kubectl -n ceph logs ceph-osd-dev-sda4-7b7c5 -c osd-prepare-pod

sshpass -p 'kevin123' pssh -h 8thors -l root -A -i ''

sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'ls'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'ps aux|grep kube'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i -t 60 'docker rmi -f $(docker images -a -q)'

#install k8s
cat <<EOF > /etc/yum.repos.d/kubernetes.repo
[kubernetes]
name=Kubernetes
baseurl=https://packages.cloud.google.com/yum/repos/kubernetes-el7-x86_64
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
EOF

sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'kubectl version; kubeadm version; kubelet --version'

sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'setenforce 0'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'sed -i 's/^SELINUX=enforcing$/SELINUX=permissive/' /etc/selinux/config'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'yum install -y kubelet-1.13.0 kubectl-1.13.0 kubeadm-1.13.0 kubernetes-cni-0.6.0'
#sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'yum install -y kubelet kubeadm kubectl --disableexcludes=kubernetes'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'systemctl enable --now kubelet'

#start k8s
sudo kubeadm init --apiserver-advertise-address=192.168.0.200 --pod-network-cidr=172.16.0.0/16 

kubectl apply -f calico.yaml

#setup k8s UI
kubectl proxy --port=9999 --address='192.168.0.200' --accept-hosts="^*$" &

#setup ssh tunnel
on tna: 
ssh -p 3775 -L 1234:thor0:8001 hydra cat -
on localmachine
ssh -p 807 -L 1234:localhost:1234 tna cat -

#common commands

kubectl -n rook-ceph get pod -l "app=rook-ceph-tools"
kubectl -n rook-ceph exec -it $(kubectl -n rook-ceph get pod -l "app=rook-ceph-tools" -o jsonpath='{.items[0].metadata.name}') bash
ceph status
ceph osd status
ceph df
rados df

kubectl get nodes
kubeadm reset
helm reset --force

sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'kubeadm reset --force'

journalctl -xeu kubelet

kubectl get pod -o=custom-columns=NAME:.metadata.name,STATUS:.status.phase,NODE:.spec.nodeName --all-namespaces

kubectl apply -f "https://cloud.weave.works/k8s/scope.yaml?k8s-version=$(kubectl version | base64 | tr -d '\n')"

# get token phase
kubeadm token create --print-join-command

#create a new device
sudo fdisk /dev/sda
n
p
+5G
sudo partprobe
sudo mkfs.ext4 /dev/sda4

#create ceph 

sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'mount /dev/sda4 /home/nannan/tmpdir'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'rm -rf /home/nannan/tmpdir/*'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'umount /home/nannan/tmpdir/'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'df -h'

helm init
helm serve &
helm repo add local http://localhost:8879/charts
kubectl create namespace ceph

kubectl create -f ceph-helm/ceph/rbac.yaml

kubectl label node thor17 ceph-mon=enabled ceph-mgr=enabled
kubectl label node thor18 ceph-mon=enabled

kubectl label node thor15 ceph-osd=enabled 
kubectl label node thor16 ceph-osd=enabled 
kubectl label node thor21 ceph-osd=enabled 

helm install --name=ceph local/ceph --namespace=ceph -f ~/ceph-overrides.yaml


sudo yum install python-devel
sudo pip install locustio==0.7.5

### install calicoctl

https://docs.projectcalico.org/v2.6/usage/calicoctl/install-and-configuration

