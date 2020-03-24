
kubectl create namespace sock-shop

kubectl apply -f user-db-dep.yaml
kubectl apply -f user-db-svc.yaml

kubectl apply -f front-end-dep.yaml
kubectl apply -f front-end-svc.yaml

