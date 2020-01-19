
udisksctl power-off -b /dev/sdX

parted /dev/sdx
mklabel gpt
yes
unit TB
mkpart primary 0 4TB
print
quit

mkfs.ext4 /dev/sdx1

#create a raid
mdadm --create /dev/md0 --level=5 --raid-devices=3 /dev/sdb1 /dev/sdc1 /dev/sdd1

#create VDO

yum install vdo kmod-kvdo

rpm -ql kmod-kvdo| grep modules
insmod /lib/modules/3.10.0-1062.el7.x86_64/extra/kmod-kvdo/uds/uds.ko

umount /home/nannan/testing/layers

vdo stop --name=vdolvm10g
vdo remove --name=vdolvm10g
vdo create --name=vdolvm10g --device=/dev/sda5 --vdoLogicalSize=10G --writePolicy=async

mount /dev/mapper/vdolvm10g /home/nannan/testing/layers

vdo create --name=vdolvm --device=/dev/md0 --vdoLogicalSize=20T --writePolicy=async
ls -l /dev/mapper/vdolvm

#format
sudo mkfs.xfs -K /dev/mapper/vdolvm10g

# check
vdostats --human-readable /dev/mapper/vdolvm

# vdo restart
vdo start --all
# dedup with btrfs/lessfs/sdfs/zfs
sudo vdo enableDeduplication --name=vdolvm250g

#============setup zfs
sudo apt install zfs
sudo zpool create -f zfsvlm /dev/sda1
sudo zpool status

sudo zfs set mountpoint=/home/nannan/sampled/ zfsvlm

zpool list zfsvlm

zfs set dedup=on zfsvlm

#===========setup btrfs
apt install btrfs-tools
mkfs.btrfs /dev/sda2

watch 'btrfs filesystem df ./sampled-2'

