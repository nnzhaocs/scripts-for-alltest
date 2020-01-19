

# ssh login without password

a@A:~> ssh-keygen -t rsa
a@A:~> ssh b@B mkdir -p .ssh
a@A:~> cat .ssh/id_rsa.pub | ssh b@B 'cat >> .ssh/authorized_keys'
a@A:~> ssh b@B

#-------
cd docker-remetrics/
sshpass -p 'nannan' pssh -h thors.lst -l root -A -i 'ceph -v'

# install ceph
1. first setup ceph-deploy
https://docs.ceph.com/docs/master/start/quick-start-preflight/#ceph-deploy-setup
2. install ceph
https://docs.ceph.com/docs/master/start/quick-ceph-deploy/

mkdir ceph-cluster
cd ceph-cluster

ceph-deploy purge thor0 thor1 thor2 thor3 thor4
ceph-deploy purgedata thor0 thor1 thor2 thor3 thor4
ceph-deploy forgetkeys
rm ceph.*

# ------------------------------------------------------------------
ceph-deploy new thor0
ceph-deploy install --release nautilus thor0 thor1 thor2 thor3 thor4

pssh -h ods -l root -A -i 'ceph -v'

ceph-deploy mon create-initial
ceph-deploy admin thor0

pssh -h ods -l root -A -i 'ceph-volume lvm zap --destroy /dev/sda5'
#need to create sda5 again

ceph-deploy osd create --data /dev/sda5 thor1
ceph-deploy osd create --data /dev/sda5 thor2
ceph-deploy osd create --data /dev/sda5 thor3
ceph-deploy osd create --data /dev/sda5 thor4

ceph-deploy mon add xxx (optional)
ceph-deploy mgr create thor2

ceph health
# -------------------------------------------------------------------
Exericse:

echo {Test-data} > testfile.txt
ceph osd pool create mytest 3
#rados put {object-name} {file-path} --pool=mytest
rados put test-object-1 testfile.txt --pool=mytest

rados -p mytest ls
#ceph osd map {pool-name} {object-name}
ceph osd map mytest test-object-1

rados rm test-object-1 --pool=mytest
ceph osd pool rm mytest
# --------------------------------------------------------------------
3. create pool and blk storage system

ceph osd pool create rbd 32
rbd pool init rbd

rbd create foo --size 1024 --image-feature layering -m thor0 -k ./ceph.client.admin.keyring -p rbd
rbd map foo --name client.admin -m thor0 -k ./ceph.client.admin.keyring -p rbd

# ---------------------------------------------------------------------
4. create filesystem
# https://docs.ceph.com/docs/master/start/quick-cephfs/ 

ceph-deploy mds create thor0

ceph osd pool create cephfs_data 32
ceph osd pool create cephfs_meta 32
ceph fs new mycephfs cephfs_meta cephfs_data

ceph osd pool set cephfs_data size 3
ceph osd pool set cephfs_meta size 3

yum install ceph-fuse
ceph-fuse /mnt/mycephfs
ceph-fuse /mnt/mycephfs/ --client_mds_namespace mycephfs

# -------------------------------------------------------------------
# create osd
vgs
lvremove xxx

mkfs.ext4 /dev/sda4
mount -o user_xattr /dev/sda4 /var/lib/ceph/osd/ceph-x
ceph-volume lvm zap /dev/sdX
ceph-volume lvm prepare --osd-id {id} --data /dev/sdX
ceph-volume lvm activate {id} {fsid}

or 

ceph-volume lvm create --osd-id {id} --data /dev/sdX

# remove osd

ceph osd out 0
ssh {osd-host}
sudo systemctl stop ceph-osd@{osd-num}


#ssh -t root@thor3 'systemctl stop ceph-osd@2'
ceph osd crush remove osd.0
ceph auth del osd.0
ceph osd rm osd.0
ceph-volume lvm zap --destroy /dev/sdax
ceph osd purge osd.x --yes-i-really-mean-it
# check
# https://docs.ceph.com/docs/master/rados/operations/monitoring/#checking-mds-status
# https://docs.ceph.com/docs/master/start/quick-rbd/

ceph mon stat
ceph mon dump
ceph quorum_status

ceph> status

ceph osd df

# cleanup ceph pool

for i in `rados -p dedup_base ls`; do echo $i; rados -p dedup_base rm $i; done

# create pool
# https://docs.ceph.com/docs/master/rados/operations/pools/#create-a-pool

# upgrade ceph

ceph-deploy install --release nautilus thor0

# start all deaon
# https://docs.ceph.com/docs/nautilus/releases/nautilus/

systemctl restart ceph-mgr.target
systemctl restart ceph-mon.target
systemctl restart ceph-osd.target

# start osd
ceph-volume simple scan
ceph-volume simple activate --all
ceph-volume simple scan /dev/sda4

# deduplication
# https://docs.ceph.com/docs/master/dev/deduplication/


# pools
ceph osd lspools
ceph osd pool delete pool
##############################
3*100
----------- = 100 2^6
3

64

pool :17 18  19  20  21  22  14  23  15  24  16 | SUM
------------------------------------------------< - *total pgs per osd*
osd.0 35 36  35  29  31  27  30  36  32  27  28 | 361
osd.1 29 28  29  35  33  37  34  28  32  37  36 | 375
osd.2 27 33  31  27  33  35  35  34  36  32  36 | 376
osd.3 37 31  33  37  31  29  29  30  28  32  28 | 360
-------------------------------------------------< - *total pgs per pool*
SUM :128 128 128 128 128 128 128 128 128 128 128

#################################

ceph osd pool get cephfs_data pg_num

# create erasure coding 
ceph osd erasure-code-profile set newmyprofile k=2 m=2 crush-failure-domain=rack
ceph osd pool create ecpool 32 32 erasure newmyprofile
