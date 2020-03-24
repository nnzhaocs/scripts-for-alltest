
kubectl run redis-thor1 --image=redis --restart=Never --overrides='{ "apiVersion": "v1", "spec": { "nodeSelector": { "kubernetes.io/hostname": "thor1" } } }'
kubectl run redis-thor13 --image=redis --restart=Never --overrides='{ "apiVersion": "v1", "spec": { "nodeSelector": { "kubernetes.io/hostname": "thor13" } } }'

kubectl get po -o wide --all-namespaces

kubectl run traceroute --image=gophernet/traceroute --restart=Never --overrides='{ "apiVersion": "v1", "spec": { "nodeSelector": { "kubernetes.io/hostname": "thor13" } } }' -- 172.16.224.156
