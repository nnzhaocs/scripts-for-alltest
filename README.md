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
	Copy downloader files to the directory ($GOPATH/src/github.com/heroku/docker-registry-client/)
	
3. patch -p1 < patch-changes-to-manifest.patch
	Apply the patch

4.  cd && make


### Run downloader
*1. Check if the down_loader works by downloading a repo library/redis*

go run down_loader.go -operation=download_manifest -repo=library/redis -tag=latest -absfilename=./test.manifest

(outfile is absfilename)


go run down_loader.go -operation=download_blobs -repo=library/redis 
-tag=44888ef5307528d97578efd747ff6a5635facbcfe23c84e79159c0630daf16de  -absfilename=./test.tarball

(tag is also digest)

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

### setup config file in analyzer/config.py

*1. config for setup*

dest_dirname: directory with layers, manifests; <layers> and <manifests> subdirectories must exist and containe all the layers and manifest;

extracting_dir: directory that is used to store layer directories. This must be set based on compressed layer tarball size.
	
	1) use tmpfs for tarballs less than 1g.
		mount -t tmpfs -o size=50960m tmpfs /mnt/extracting_dir
		
	2) use ssd for tarballer bigger than 1g.
	
num_worker_process: number of processes. This must be set based on extracting directory size and layer tarball size.

	1) list_less_50m.json, 60
	
	2) list_less_1g.json, 20,
	
	3) list_less_2g.json, 5,
	
	4) list_bigger_2g.json, 5
	
*2. config for input files*

analyzed_absfilename: file containing list of layers that are already analyzed, newline separated layer digests

*2. Output directories no need to change*

layer_db_json_dirname: directory containing all the layer profiles, json files.

image_db_json_dirname: directory containing all the image profiles (mapper), json files

job_list_dirname: directory containing all the output files.

layer_list_absfilename: file containing list of layers to be analyzed, newline separated layer digests

layer_config_map_dir_filename: file containning a map between layer/config digest and the layer tarball path, json file.

layer_json_map_dir_filename: file containning a map between layer digest and the layer profile path, json file. 

manifest_map_dir_filename: : file containning a map between repo name and the manifest path, json file. 

dirs: a list of directories that store layers, configs, and manifests

### Supports four mutually exclusive modes:

<-L,-J,-P,-I>

-J - job devider

-L - layer analyzer

-F - file mapper

-I - image analyzer

1. Job divider (-J)

python main.py [-D] -J 

Creates a directory "job_list_dir" which contains 
four files:

	1) list_less_50m.json,
	
	2) list_less_1g.json,
	
	3) list_less_2g.json,
	
	4) list_bigger_2g.json

-D - debug

2. Layer analyzer (-L)

python main.py [-D] -L 

Creates a directory "layer_db_json_dirname" which contains 
layer profile, json file.
Creates files:

	1) analyzed_layer_filename-*.out
	
	2) bad_nonanalyzed_layer_list-*.out

-D - debug

(After finishing, joint and save all analyzed_layer_file*/bad_nonanalyzed_layer_list-*)

3. File mapper (-F)

python main.py [-D] -F 

Creates a map for layer/config digest<->tarball path; repo name<->manifest path.
Creates files:

	1) layer_config_map_dir.json
	
	2) layer_json_map_dir.json
	
	3) manifest_map_dir.json

-D - debug

4. Image analyzer (-I)

python main.py [-D] -I 

Creates a map for repo name<->manifest path,layer/config digest<->tarball path, layer profile path.
input files:

	1) layer_config_map_dir.json
	
	2) layer_json_map_dir.json
	
	3) manifest_map_dir.json

Creates files:

	1) image_mapper.json
		containning the mapper
		
	2) layer_analyzer_jobs.json
		containning layers that has no layer profile and need to be analyzed
		
	3) bad_manifests.json
		containning manifests that cannot be read

## Plotter 

-P - plot

*4. Plot_graph*

python main.py -D -P -d /gpfs/docker_images_largefs/

## Tests

*1. Downloader*

~20MB/s

*2. Analyzer*

~1s per layer
