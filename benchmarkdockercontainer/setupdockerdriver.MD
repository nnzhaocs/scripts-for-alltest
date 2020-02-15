

#==========install docker==============

https://docs.docker.com/install/linux/docker-ce/centos/

sudo yum install -y yum-utils \
  device-mapper-persistent-data \
  lvm2

sudo yum-config-manager \
    --add-repo \
    https://download.docker.com/linux/centos/docker-ce.repo

sudo yum install docker-ce docker-ce-cli containerd.io

sudo systemctl start docker

# ==== resize pv or lv if needed ====

https://community.spiceworks.com/topic/2043676-resizing-partitions-in-centos-7

https://www.tecmint.com/manage-and-create-lvm-parition-using-vgcreate-lvcreate-and-lvextend/
https://www.thegeekdiary.com/redhat-centos-a-beginners-guide-to-lvm-logical-volume-manager/

pvs
lvs
vgs

lvcreate -L 30G -n testlvm centos
mkfs.xfs /dev/centos/testlvm

thor6 /dev/sda5 devicemapper
thor16 /dev/sda6 btrfs
thor13 /dev/mapper/testlvm overlay2
thor14 /dev/mapper/testlvm zfs

#===== overlay2 =====

https://docs.docker.com/storage/storagedriver/overlayfs-driver/

{
  "storage-driver": "overlay2"
}

mount /dev/sda7 /testing/

#====== devicemapper ===

https://docs.docker.com/storage/storagedriver/device-mapper-driver/

{
  "storage-driver": "devicemapper",
  "storage-opts": [
    "dm.directlvm_device=/dev/sda5",
    "dm.thinp_percent=95",
    "dm.thinp_metapercent=1",
    "dm.thinp_autoextend_threshold=80",
    "dm.thinp_autoextend_percent=20",
    "dm.directlvm_device_force=true"
  ]
}

# create devicemapper for rawfs

pvcreate /dev/sda6

lvcreate --wipesignatures y -n thinpool testing -l 95%VG
lvcreate --wipesignatures y -n thinpoolmeta testing -l 1%VG

sudo lvconvert -y --zero n -c 512K --thinpool testing/thinpool --poolmetadata testing/thinpoolmeta

sudo lvs -o+seg_monitor
mkfs.xfs /dev/testing/thinpool

mount /dev/testing/thinpool /testing

#===== btrfs ======

yum install btrfs-progs -y
sudo mkfs.btrfs -f /dev/sda6
sudo mount -t btrfs /dev/sda6 /var/lib/docker

{
  "storage-driver": "btrfs"
}

https://docs.docker.com/storage/storagedriver/btrfs-driver/

sudo mkfs.btrfs -f /dev/sda7
mount /dev/sda7 /testing/

#======zfs=========

install zfs

https://www.symmcom.com/docs/how-tos/storages/how-to-install-zfs-on-centos-7
https://github.com/zfsonlinux/zfs/wiki/RHEL-and-CentOS

zpool create -f testlvm -m /var/lib/docker /dev/centos/testlvm

zfs list

{
  "storage-driver": "zfs"
}

zpool create -f testrawfs -m /testing /dev/sda7
