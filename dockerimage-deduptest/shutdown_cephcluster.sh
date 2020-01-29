

#ceph osd set noout
#ceph osd set norecover
#ceph osd set norebalance
#ceph osd set nobackfill
#ceph osd set nodown
#ceph osd set pause

#ceph osd unset noout
#ceph osd unset norecover
#ceph osd unset norebalance
#ceph osd unset nobackfill
#ceph osd unset nodown
#ceph osd unset pause


ceph osd out 0
ceph osd out 1
ceph osd out 2
ceph osd out 3

ssh -t root@thor1 'systemctl stop ceph-osd@0'
ssh -t root@thor2 'systemctl stop ceph-osd@1'
ssh -t root@thor3 'systemctl stop ceph-osd@2'
ssh -t root@thor4 'systemctl stop ceph-osd@3'

ceph osd purge osd.0 --yes-i-really-mean-it
ceph osd purge osd.1 --yes-i-really-mean-it
ceph osd purge osd.2 --yes-i-really-mean-it
ceph osd purge osd.3 --yes-i-really-mean-it

