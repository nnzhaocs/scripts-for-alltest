
ssh-keygen -t rsa -P ""
cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys

ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@hulk0
ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@hulk2
ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@amaranth1
ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@amaranth2
ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@amaranth3
ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@amaranth4
ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@amaranth5
ssh-copy-id -i $HOME/.ssh/id_rsa.pub nannan@amaranth6

### cat cluster_machines.lst >> /etc/hosts
### cat hadoop_env.lst >> ~/.bashrc
### source ~/.bashrc