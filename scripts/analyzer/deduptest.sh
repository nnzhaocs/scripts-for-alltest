
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
vdo create --name=vdolvm --device=/dev/md0 --vdoLogicalSize=20T --writePolicy=async
ls -l /dev/mapper/vdolvm

#format
sudo mkfs.xfs -K /dev/mapper/vdolvm

# check
vdostats --human-readable /dev/mapper/vdolvm

# vdo restart
vdo start --all
# dedup with btrfs/lessfs/sdfs/zfs
