#!/bin/bash

============run downloader as a docker image!!!==========================

mkdir ~/Downloads; touch ~/Downloads/downloadedlayers.lst; touch ~/Downloads/downloadedimages.lst

mv xxxx.lst ~/Downloads/image_name.lst

docker build ./ -t downloader

docker run -v ~/Downloads/:/root/Downloads downloader

==========================================================================
date >> timestamp.out
python auto_download_compressed_images.py -f images-names/xdj -d /gpfs/docker_images_largefs/xdj/
date >> timestamp.out
python auto_download_compressed_images.py -f images-names/xdh -d /gpfs/docker_images_largefs/xdh/
date >> timestamp.out
python auto_download_compressed_images.py -f images-names/xdg -d /gpfs/docker_images_largefs/xdg/
date >> timestamp.out
python auto_download_compressed_images.py -f images-names/xdf -d /gpfs/docker_images_largefs/xdf/
date >> timestamp.out
