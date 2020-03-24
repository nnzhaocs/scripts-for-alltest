
# sshpass -p 'kevin123' pssh -h 8thors -l root -A -i 'chown -R nannan:nannan /home/nannan'
# sshpass -p 'kevin123' pssh -h 8thors -l root -A -i 'ls -al /home/nannan'
# ssh-keygen
# ssh-copy-id nannan@thor21

#helm init
#sleep 1
#helm serve &
#
#sleep 1
#helm repo add local http://localhost:8879/charts
#
#sleep 1
#kubectl create namespace ceph
#
#sleep 1
#kubectl create -f ceph-helm/ceph/rbac.yaml

#sleep 1

echo "using ceph fs"

cd /home/nannan/containers/rook/
cd cluster/examples/kubernetes/ceph
kubectl create -f common.yaml
kubectl create -f operator.yaml

sleep 1

kubectl label node thor17 ceph-mon=enabled ceph-mgr=enabled
kubectl label node thor18 ceph-mon=enabled

sleep 1
kubectl label node thor15 ceph-osd=enabled 
kubectl label node thor16 ceph-osd=enabled 
kubectl label node thor21 ceph-osd=enabled 

kubectl create -f cluster.yaml

sleep 240

kubectl create -f filesystem.yaml

sleep 10

kubectl create -f toolbox.yaml

kubectl -n rook-ceph get pod -l app=rook-ceph-mds
#sleep 3
#helm install --name=ceph local/ceph --namespace=ceph -f /home/nannan/ceph-overrides.yaml

cd /home/nannan/containers/rook/cluster/examples/kubernetes/ceph/csi/cephfs

kubectl create -f storageclass.yaml

sleep 1

kubectl create namespace sock-shop

kubectl create -f pvc-rwx.yaml

#kubectl create -f kube-registry.yaml

kubectl get pvc,pv

#kubeclt delete -f kube-registry.yaml
