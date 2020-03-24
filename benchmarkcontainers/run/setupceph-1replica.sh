
echo "using ceph fs"

cd /home/nannan/containers/rook/
cd cluster/examples/kubernetes/ceph
kubectl create -f common.yaml
kubectl create -f operator.yaml

sleep 1

#kubectl label node thor17 ceph-mon=enabled ceph-mgr=enabled --overwrite=true
#kubectl label node thor18 ceph-mon=enabled

sleep 1
#kubectl label node thor15 ceph-osd=enabled --overwrite=true
#kubectl label node thor16 ceph-osd=enabled 
#kubectl label node thor21 ceph-osd=enabled 

kubectl create -f cluster-test.yaml

sleep 180

kubectl create -f csi/rbd/storageclass.yaml
#kubectl create -f filesystem-1replica.yaml

sleep 10

kubectl create -f toolbox.yaml

kubectl get pod -o=custom-columns=NAME:.metadata.name,STATUS:.status.phase,NODE:.spec.nodeName --all-namespaces

#kubectl -n rook-ceph get pod -l app=rook-ceph-mds

#cd /home/nannan/containers/rook/cluster/examples/kubernetes/ceph/csi/cephfs

#kubectl create -f storageclass.yaml

sleep 1

kubectl create -f /home/nannan/containers/rook/cluster/examples/kubernetes/mysql.yaml

#kubectl create -f kube-registry.yaml

kubectl get pvc,pv -n rook-ceph

#kubectl delete -f kube-registry.yaml
