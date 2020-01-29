
# Create filesystem
# https://docs.ceph.com/docs/master/start/quick-cephfs/

ceph-deploy mds create thor0

ceph osd pool create cephfs_data 32
ceph osd pool create cephfs_meta 32
ceph fs new iotest_cephfs cephfs_meta cephfs_data

# 3-way replication

ceph osd pool set cephfs_data size 3
ceph osd pool set cephfs_meta size 3

yum install ceph-fuse
ceph-fuse /home/nannan/testing/
ceph-fuse /home/nannan/testing/ --client_mds_namespace mycephfs

# remove fs and associated pools
# http://lists.suse.com/pipermail/deepsea-users/2017-June/000143.html

systemctl stop ceph-mds.target
ceph fs ls
ceph fs rm mycephfs  --yes-i-really-mean-it
ceph osd pool ls
ceph osd pool rm cephfs_data cephfs_data --yes-i-really-really-mean-it
#ceph osd pool rm cephfs_metadata cephfs_metadata --yes-i-really-really-mean-it
ceph osd pool rm cephfs_meta cephfs_meta --yes-i-really-really-mean-it
ceph osd pool ls 

systemctl start ceph-mds.target

# delete pools
# https://documentation.suse.com/ses/6/html/ses-all/ceph-pools.html
# https://blog.widodh.nl/2015/04/protecting-your-ceph-pools-against-removal-or-property-changes/

for pool in $(rados lspools); do ceph osd pool set $pool nodelete false; done
ceph tell mon.* injectargs --mon-allow-pool-delete=true

