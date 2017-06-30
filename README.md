# docker-remetrics
A Tool for downloading all the images from docker hub and analyzing these images 

## Intro
The project docker-remetrics is to download all the images from docker hub, analyze and characterize them. 

## Installation
### Requirments 
*Go 1.7.4 or above*

*Python 2.7.4 or above*

### setup docker-registry-client lib and downloader, assuming $GOPATH=../go/
1. go get -v github.com/heroku/docker-registry-client.git
2. apply two ****.patch file to ../go/src/github.com/heroku/docker-registry-client/registry/manifest.go
3. cp down_loader.go auto_download_compressed_images.py ../go/src/github.com/heroku/docker-registry-client/
3. make
### Run downloader
*1. Check if the down_loader works by downloading a repo library/redis*

go run down_loader.go -operation=download_manifest -filename=library/redis -tag=latest -dirname=./test

go run down_loader.go -operation=download_blobs -filename=library/redis -tag=44888ef5307528d97578efd747ff6a5635facbcfe23c84e79159c0630daf16de  -dirname=./test

*operation: download_manifest or download_blobs*

*filename: image name, namespace/reponame*

*tag: tag or digest*

*dirname: output filename, manifest file name or layer blobs'name*

*2. Run the downloader*

python auto_download_compressed_images.py -f xdl -d /gpfs/docker_images_largefs/xdl

### Run analyzer
1. 

## Tests

