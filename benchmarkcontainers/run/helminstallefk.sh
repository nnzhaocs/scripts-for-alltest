helm init

#export HELM_HOST=":44134"
#tiller -listen ${HELM_HOST} -alsologtostderr >/dev/null 2>&1 &
helm repo add stable https://kubernetes-charts.storage.googleapis.com

helm install elk stable/elastic-stack

export POD_NAME=$(kubectl get pods --namespace default -l "app=elastic-stack,release=elk" -o jsonpath="{.items[0].metadata.name}")
echo "Visit http://127.0.0.1:5601 to use Kibana"
kubectl port-forward --namespace default $POD_NAME 5601:5601
helm delete my-release
kubectl delete pvc -l release=my-release,component=data

sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'sysctl -w vm.max_map_count=262144'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'sysctl --system'
sshpass -p 'kevin123' pssh -h 9thors -l root -A -i 'sysctl vm.max_map_count'

sudo sysctl -w vm.max_map_count=262144
sysctl vm.max_map_count
sudo sysctl --system

helm-v3.0.0-linux-amd64.tar.gz
#helm-v2.13.0-rc.2-linux-amd64.tar.gz

sudo rm -rf /usr/bin/helm
sudo rm -rf /usr/local/bin/helm

sudo rm -rf /usr/bin/tiller
sudo rm -rf /usr/local/bin/tiller

sudo mv linux-amd64/helm /usr/local/bin/helm
cd /usr/local/bin/
sudo cp helm /usr/bin/helm

kubectl get events
