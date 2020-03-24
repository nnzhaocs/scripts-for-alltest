
echo "gathering all the container logs"

dir=$(date +%Y%m%d_%H%M%S)
echo $dir

mkdir -p ./logging/$dir

kubectl -n sock-shop logs deployment/front-end --all-containers=true &> ./logging/$dir/tmp.log
ansifilter ./logging/$dir/tmp.log > ./logging/$dir/front-end.log

kubectl -n sock-shop logs deployment/carts --all-containers=true &> ./logging/$dir/tmp.log
ansifilter ./logging/$dir/tmp.log > ./logging/$dir/carts.log

kubectl -n sock-shop logs deployment/catalogue --all-containers=true &> ./logging/$dir/tmp.log
ansifilter ./logging/$dir/tmp.log > ./logging/$dir/catalogue.log

kubectl -n sock-shop logs deployment/user --all-containers=true &> ./logging/$dir/tmp.log
ansifilter ./logging/$dir/tmp.log > ./logging/$dir/user.log

kubectl -n sock-shop logs deployment/orders --all-containers=true &> ./logging/$dir/tmp.log
ansifilter ./logging/$dir/tmp.log > ./logging/$dir/orders.log

kubectl -n sock-shop logs deployment/carts-db --all-containers=true &> ./logging/$dir/carts-db.log
kubectl -n sock-shop logs deployment/catalogue-db --all-containers=true &> ./logging/$dir/catalogue-db.log
kubectl -n sock-shop logs deployment/user-db --all-containers=true &> ./logging/$dir/user-db.log
kubectl -n sock-shop logs deployment/orders-db --all-containers=true &> ./logging/$dir/orders-db.log

kubectl -n sock-shop logs deployment/payment --all-containers=true &> ./logging/$dir/payment.log

kubectl -n sock-shop logs deployment/queue-master --all-containers=true &> ./logging/$dir/queue-master.log
kubectl -n sock-shop logs deployment/rabbitmq --all-containers=true &> ./logging/$dir/rabbitmq.log
kubectl -n sock-shop logs deployment/shipping --all-containers=true &> ./logging/$dir/shipping.log





