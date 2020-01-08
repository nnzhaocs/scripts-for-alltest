

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
