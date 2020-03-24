
cd /home/nannan/containers/rook/cluster/examples/kubernetes/ceph/

kubectl delete -f ../wordpress.yaml
kubectl delete -f ../mysql.yaml

sleep 1

kubectl delete -f csi/cephfs/pvc.yaml
kubectl delete -f csi/cephfs/pvc-rwx.yaml

kubectl delete -f csi/cephfs/kube-registry.yaml
kubectl -n rook-ceph delete cephfilesystem myfs

sleep 1

kubectl delete -n rook-ceph cephblockpool replicapool
kubectl delete storageclass rook-ceph-block

kubectl delete storageclass csi-cephfs
sleep 1

kubectl delete storageclass rook-cephfs

kubectl -n rook-ceph delete cephcluster rook-ceph
kubectl -n rook-ceph get cephcluster
kubectl -n rook-ceph get cephcluster
sleep 1

kubectl delete -f cluster.yaml
kubectl delete -f operator.yaml
kubectl delete -f common.yaml

sleep 1

cd /home/nannan/containers/run/
#sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'mount /dev/sda4 /home/nannan/tmpdir'
#sleep 1
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'rm -rf /home/nannan/tmpdir/*'
sleep 1
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'rm -rf /home/nannan/ceph-storage-dir/*'
#sleep 1
#sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'df -h'

kubectl -n rook-ceph get pod
kubectl -n rook-ceph get cephcluster


