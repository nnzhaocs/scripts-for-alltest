# docker-remetrics
A Tool for downloading all the images from docker hub and analyzing these images 

## Intro
The project docker-remetrics is to download all the images from docker hub, analyze and characterize them. 

## Installation
### Requirments 
*Go 1.7.4 or above*

*Python 2.7.4 or above*

### Install
1. first copy to dir
2. run: make

### Run downloader
*1. Check if the down_loader works by downloading a repo library/redis*

go run down_loader.go -operation=download_manifest -filename=library/redis -tag=latest -dirname=./test

go run down_loader.go -operation=download_blobs -filename=library/redis -tag=44888ef5307528d97578efd747ff6a5635facbcfe23c84e79159c0630daf16de  -dirname=./test

*2. Run the downloader*

python auto_download_compressed_images.py -f xdl -d /gpfs/docker_images_largefs/xdl

### Run analyzer
1. 

## Tests

