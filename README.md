# docker-remetrics
A Tool for downloading all the images from docker hub and analyzing these images.

## Installation
### Requirments 
*Go 1.7.4 or above*

*Python 2.7.4 or above*

### Setup docker-registry-client lib and downloader
1. go get -v github.com/heroku/docker-registry-client
2. cp down_loader.go auto_download_compressed_images.py ****.patch $GOPATH/src/github.com/heroku/docker-registry-client/
3. git am ****.patch //apply ****.patch file to $GOPATH/src/github.com/heroku/docker-registry-client/registry/manifest.go   
4. make
### Run downloader
*1. Check if the down_loader works by downloading a repo library/redis*

go run down_loader.go -operation=download_manifest -repo=library/redis -tag=latest -absfilename=./test.manifest

go run down_loader.go -operation=download_blobs -repo=library/redis 
-tag=44888ef5307528d97578efd747ff6a5635facbcfe23c84e79159c0630daf16de  -absfilename=./test.tarball

*2. Run the downloader to massively downloading the repos*

root# python auto_download_compressed_images.py -f xdl -d /gpfs/docker_images_largefs/xdl

*Note! xdl is the input file that contains images'names, please use the files in images-names directory/repo which follows the correct format*

### Run analyzer
1. 

## Tests

