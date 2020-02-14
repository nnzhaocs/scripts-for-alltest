
# ========== install fio  ==========================
wget https://github.com/axboe/fio/archive/fio-3.6.tar.gz
tar -zxvf fio

# if not

sudo yum groupinstall 'Development Tools'

./configure
 make
 make install

# ============== install hdparm and bc =======================

yum install bc hdparm

