# docker-remetrics
A Tool for downloading all the images from docker hub and analyzing these images.

## Installation
### Requirments 
*Go 1.7.4 or above*

*Python 2.7.4 or above, and some python packages*

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

*2. Run the downloader to massively download the repos*

root# python auto_download_compressed_images.py -f unique_name.out -d /gpfs/docker_images_largefs/ -l /home/nannan/docker-remetrics/downloader/finished_layer_list.out -r /home/nannan/docker-remetrics/downloader/finished_repo_list.out &> downloader-8-2.log &

### Run analyzer

*1. analyze tarballs less than 50MB

mount -t tmpfs -o size=50960m tmpfs /mnt/extracting_dir
python main.py -D -L -d /gpfs/docker_images_largefs/ -a analyzed_layer_file-less-50m.out -s /gpfs/docker_images_largefs/job_list_dir/list_less_50m.out  -e /mnt/extracting_dir/ &> analyzer_less-50m-8-2.log &

*number of workers: less than 50mb(60), less than 1g(20), less than 2g(5)

*2. analyze tarballs larger than 2g

python main.py -D -L -d /gpfs/docker_images_largefs/ -a analyzed_layer_file-bigger-2g.out -s /gpfs/docker_images_largefs/job_list_dir/list_bigger_2g.out  -e /mnt/largerssd/ &> analyzer_bigger-1g-8-2.log &

*3. analyze manifest: todo

## Tests

*1. Downloader*

~20MB/s

*2. Analyzer*

~1s per layer
