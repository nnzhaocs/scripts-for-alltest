# docker-remetrics

Tool for downloading images from Docker Hub and analyzing them.

This package contains three main components:

1) Crowler crawls Docker Hub search page and extracts the
   list of all available public repositories.

2) Downloader that downloads images and layers from the registry.

3) Analyzer that scans all layers and images and produces
   JSON files with various metrics.

4) Plotter that takes JSON files as an input and plots
   various aggregated statistics.

## Installation

All components require Python 2.7.4 or above. In addition,
some components require some additional python and other packages.

To install Python on CentOS 7.3:

`yum install go python`

TODO: split README (and code, if needed) based on requirements:

### Crawler

No additional dependencies.

### Downloader:

Go 1.7.4 or above (GOPATH variable must be set properly).

`yum install go`

TODO: Explain what exactly should be done
setup GOPATH

### Analyzer 

Analyzer essentially needs python-magic package to recognize
file type based on the magic number. There are two versions
of this package. We use the one installed via pip, not via yum!

In addition, Analyzer requires statistics package to compute
various metrics during analysis. This package must also
be installed using pip.

To install pip on CentOS 7.3:

`sudo yum -y install epel-release`
`sudo yum -y install python-pip`

To install python-magic and statistics

`sudo pip install python-magic`
`sudo pip install statistic`

### Plotter

Plotter requires statistics and matplotlib packages. Statistics package
comes from pip, while matplotlib should be installed usin yum:

`sudo yum install python-matplotlib`
`sudo pip install statistics`

## Crowler

TODO: Explain how to use and what it produces.

## Downloader

Downloader relies on a heroku/docker-registry-client library to download
images and layers. This library allows to use Registry REST API directly
without running full Docker client.

However, we needed to modify the original library because it only
could download manifests of schema 2 (not multiarch), but we wanted to also
download schema 1 and multiarchitect schema 2. The instructions
below explain how to modify and compile updated heroku/docker-registry-client
version.

### List of files

`down_loader.go` - short description
`auto_download_compressed_images.py` - short description
`patch-changes-to-manifest.patch` - short description

XXX: Modify to be able to graciously to shutdown the downloading process.

### Setup updated docker-registry-client

1.  Download heroku/docker-registry-client from github:

`go get -v github.com/heroku/docker-registry-client`

2.  Copy patch and downloader files to $GOPATH/src/github.com/heroku/docker-registry-client/ :

`cp down_loader.go auto_download_compressed_images.py patch-changes-to-manifest.patch $GOPATH/src/github.com/heroku/docker-registry-client/`
	
3. Apply the patch:
 
`patch -p1 < patch-changes-to-manifest.patch`

4. Compile the library

`cd && make`

### Downloader run example

*1. Check if the downloader works by downloading  library/redis repo*

`go run down_loader.go -operation=download_manifest -repo=library/redis -tag=latest -absfilename=./test.manifest`

`go run down_loader.go -operation=download_blobs -repo=library/redis 
-tag=44888ef5307528d97578efd747ff6a5635facbcfe23c84e79159c0630daf16de  -absfilename=./test.tarball`

*2. Run the downloader to massively download the repos*

`mkdir -p /tmp/downloaded/layers`
`mkdir -p /tmp/downloaded/configs`
`touch  /tmp/downloaded-layers.lst`
`touch  /tmp/downloaded-images.lst`

`python auto_download_compressed_images.py
	-f /tmp/repos_to_download.lst
	-d /tmp/downloaded/
	-l /tmp/downloaded-layers.lst 
	-r /tmp/downloaded-images.lst &> downloader.log &`


### Explanation of command line parameters

#### `down_loader.go`

TODO: fill this in

#### `auto_download_compressed_images.py`

TODO: finish this list below and polish

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
