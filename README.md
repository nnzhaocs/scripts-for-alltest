# docker-remetrics
A Tool for downloading all the images from docker hub and analyzing these images.

## Installation
### Requirments 

TODO: split README (and code, if needed) based on requirements:

For Downloader:

*Go 1.7.4 or above, and setup GOPATH*

*Python 2.7.4 or above, and some python packages*

yum install go python

setup GOPATH

For Analyzer & Plotter:

* for example for CentOS 7.3*

sudo yum -y install epel-release
	for pip (which we later use for installing statistics package)

sudo yum -y install python-pip
	install pip from epel

sudo pip install python-magic
	INSTALL USING PIP
	for file magic detection
	ATTENTION: there are two Python packages named "magic" we must
	use the one that runs via pip not via RPM.

sudo yum install python-matplotlib
	for plotting but imported in downloader and analyzer as well


sudo pip install statistics

## Crowler

Explain how to use and what it produces.

## Downloader

### List of files

xxx - short description
xxx - short description
xxx - short description

XXX: Modify to be able to graciously to shutdown the downloading process.

### Setup docker-registry-client lib and downloader

1. go get -v github.com/heroku/docker-registry-client
	Download the library from github.

2. cp down_loader.go auto_download_compressed_images.py ****.patch $GOPATH/src/github.com/heroku/docker-registry-client/
	Copy downloader files to the directory (XXX)
	
3. patch -p1 < patch-changes-to-manifest.patch
	Apply the patch

4.  cd && make


### Run downloader
*1. Check if the down_loader works by downloading a repo library/redis*

go run down_loader.go -operation=download_manifest -repo=library/redis -tag=latest -absfilename=./test.manifest

(XXX: outfile is absfilename)


go run down_loader.go -operation=download_blobs -repo=library/redis 
-tag=44888ef5307528d97578efd747ff6a5635facbcfe23c84e79159c0630daf16de  -absfilename=./test.tarball

(XXX: tag is confusing)

*2. Run the downloader to massively download the repos*

mkdir -p /tmp/downloaded/layers
mkdir -p /tmp/downloaded/configs
touch  /tmp/downloaded-layers.lst
touch  /tmp/downloaded-images.lst

root# python auto_download_compressed_images.py
	-f /tmp/repos_to_download.lst
	-d /tmp/downloaded/
	-l /tmp/downloaded-layers.lst 
	-r /tmp/downloaded-images.lst &> downloader.log &

-f <file containing the list of repositories to download>
	The format of the file is CSV with the first column is star count, second column
	is a pull count and the last (third) column is the name of the repo.
	XXX: Where does this file come from? Explain.
-d <directory where to put manifests, configs, and layers. configs/ and layers/ subdirecotries must exist>
-l <file containing the list of layer digests that are already downloaded, newline separated digests>
	This file is read AND appended with newly downloaded layers
-r <file containing the list of repositories that are already downloaded>
	This file is read AND appended with newly downloaded images

## Analyzer

### Run analyzer

XXX: rename main.py

Supports four mutually exclusive modes:

<-L,-J,-P,-I>

-J - job devider
-L - layer analyzer
-I - image analyzer

## Plotter 

-P - plot

1. Job divider (-J)

python main.py [-D] -J -d /gpfs/docker_images_largefs/

Creates a directory "job_list_dir" which contains 
four files:

	1) list_less_50m.json,
	2) list_less_1g.json,
	3) list_less_2g.json,
	4) list_bigger_2g.json

-D - debug
-d - directory with layers; <layers> subdirectory must exist and containe all the layers



*1. Analyze tarballs less than 50MB*

mount -t tmpfs -o size=50960m tmpfs /mnt/extracting_dir

python main.py -D -L -d /gpfs/docker_images_largefs/ -a analyzed_layer_file-less-50m.out -s /gpfs/docker_images_largefs/job_list_dir/list_less_50m.out  -e /mnt/extracting_dir/ &> analyzer_less-50m-8-2.log &

*number of workers: less than 50mb(60), less than 1g(20), less than 2g(5)*

*2. Analyze tarballs bigger than 2g*

python main.py -D -L -d /gpfs/docker_images_largefs/ -a analyzed_layer_file-bigger-2g.out -s /gpfs/docker_images_largefs/job_list_dir/list_bigger_2g.out  -e /mnt/largerssd/ &> analyzer_bigger-1g-8-2.log &


*4. Plot_graph*

python main.py -D -P -d /gpfs/docker_images_largefs/

## Tests

*1. Downloader*

~20MB/s

*2. Analyzer*

~1s per layer
