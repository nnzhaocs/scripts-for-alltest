
kubectl delete pod user-55986d48d6-pslkr --grace-period=0 --force -n sock-shop
systemctl status kubelet

cd /home/nannan/containers/microservices-demo/deploy/kubernetes/
kubectl create namespace sock-shop

thor0:
kubectl create -f complete-demo-samenodes.yaml
kubectl delete -f complete-demo-samenodes.yaml

kubectl create -f complete-demo-diffnodes.yaml
kubectl delete -f complete-demo-diffnodes.yaml

thor23: ./runLocust.sh -h 192.168.0.213:30001 -c 50 -r 1000
