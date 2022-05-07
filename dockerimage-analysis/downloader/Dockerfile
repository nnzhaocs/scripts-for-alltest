From fedora:latest
#RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN yum update -y & yum install -y wget
#& yum install golang-1.13.6-1.el7.x86_64.rpm
#RUN yum install -y wget 
RUN wget https://dl.google.com/go/go1.13.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.13.linux-amd64.tar.gz
ENV PATH="${PATH}:/usr/local/go/bin" 
# >> ~/.bash_profile & source ~/.bash_profile
#RUN cat ~/.bash_profile
#RUN echo $PATH
RUN mkdir ~/go
#; rm -rf ~/go1.13.linux-amd64.tar.gz
RUN dnf -y update & dnf install -y python2 & dnf install -y git
#| grep -i python
#
#RUN dnf module install python27
#RUN alternatives --install /usr/lib/alternatives/python python 
#RUN alternatives --set python /usr/bin/python2
RUN python --version
#RUN whereis go
#RUN echo $PATH
RUN go version
#RUN dnf install -y git
RUN git config --global http.version HTTP/1.1; go get -v github.com/nnzhaocs/docker-registry-client; exit 0
#go env -w GOPROXY=https://goproxy.cn,direct & go get -v -d github.com/nnzhaocs/docker-registry-client; 
#exit 0
RUN cd ~; git clone https://github.com/nnzhaocs/scripts-for-alltest.git
RUN ls ~/scripts-for-alltest
RUN ls ~/go/src/github.com/nnzhaocs/
RUN cp ~/scripts-for-alltest/dockerimage-analysis/downloader/down_loader.go ~/scripts-for-alltest/dockerimage-analysis/downloader/auto_download_compressed_images.py ~/scripts-for-alltest/dockerimage-analysis/downloader/patch-changes-to-manifest.patch ~/go/src/github.com/nnzhaocs/docker-registry-client
RUN dnf -y install patch; cd ~/go/src/github.com/nnzhaocs/docker-registry-client/; patch -p1 < patch-changes-to-manifest.patch

RUN mkdir -p /root/Downloads
VOLUME ["/root/Downloads/"]
WORKDIR /root/go/src/github.com/nnzhaocs/docker-registry-client/
ENTRYPOINT ["python2", "/root/scripts-for-alltest/dockerimage-analysis/downloader/auto_download_compressed_images.py"]
CMD ["-f", "/root/Downloads/image_name.lst", "-d", "/root/Downloads/", "-l", "/root/Downloads/downloadedlayers.lst", "-r", "/root/Downloads/downloadedimages.lst"]

