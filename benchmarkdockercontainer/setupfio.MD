
# ========== install fio  ==========================
wget https://github.com/axboe/fio/archive/fio-3.6.tar.gz
tar -zxvf fio

# if not

sudo yum groupinstall 'Development Tools'

./configure
 make
 make install

# ============== install hdparm and bc =======================

sshpass -p 'kevin123' pssh -h thors.lst -l root -A -i 'cd /home/nannan/fio-fio-3.6; ./configure'

yum install bc hdparm

yum install libaio-devel

