

# https://docs.ceph.com/docs/master/dev/deduplication/
# https://github.com/ceph/ceph/blob/1a379bc20227322154d225b3712df49242c05db3/doc/dev/deduplication.rst
# https://forum.proxmox.com/threads/ceph-deduplication-14-2-4.58631/

rados -p base_pool put test-base ./ods1
rados -p chunk_pool put test-chunk2 ./ods

rados -p base_pool set-chunk test-base 0 20 --target-pool chunk_pool test-chunk2 0 --with-reference
rados -p base_pool set-redirect <base_object> --target-pool <target_pool>
 <target_object>

rados ls -p chunk_pool
rados ls -p base_pool

#-----intergeted ceph with sift
ceph osd pool create cephfs_data 32
ceph osd pool create cephfs_meta 32
ceph fs new mycephfs cephfs_meta cephfs_data

ceph osd erasure-code-profile set ec-22-profile k=2 m=2 crush-failure-domain=host crush-device-class=ssd
ceph osd pool create cephfs_data_ec22 32 erasure ec-22-profile
ceph osd pool set cephfs_data_ec22 allow_ec_overwrites true
ceph fs add_data_pool mycephfs cephfs_data_ec22
ceph-deploy mds create thor0
ceph-fuse /home/nannan/testing/layers/ --client_mds_namespace mycephfs
setfattr -n ceph.dir.layout -v pool=cephfs_data_ec22 /home/nannan/testing/layers



