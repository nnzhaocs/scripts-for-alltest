

#install k8s
#cat <<EOF > /etc/yum.repos.d/kubernetes.repo
#[kubernetes]
#name=Kubernetes
#baseurl=https://packages.cloud.google.com/yum/repos/kubernetes-el7-x86_64
#enabled=1
#gpgcheck=1
#repo_gpgcheck=1
#gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
#EOF

sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -i 'kubectl version; kubeadm version; kubelet --version'

sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -i 'setenforce 0'
sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -i 'sed -i 's/^SELINUX=enforcing$/SELINUX=permissive/' /etc/selinux/config'
sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -i 'yum install -y kubelet-1.13.0 kubectl-1.13.0 kubeadm-1.13.0 kubernetes-cni-0.6.0'
#sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -i 'yum install -y kubelet kubeadm kubectl --disableexcludes=kubernetes'
sshpass -p 'kevin123' pssh -h newworkers.lst -l root -A -i 'systemctl enable --now kubelet'

###### uinstall k8s #########
# sudo yum remove kubeadm kubectl kubelet kubernetes-cni kube*


