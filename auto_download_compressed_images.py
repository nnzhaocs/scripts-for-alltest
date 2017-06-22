import os, sys, subprocess, select, random, urllib2, time, json, tempfile, shutil
import re
import threading, Queue
import argparse
from optparse import OptionParser
import requests

q = Queue.Queue()
num_worker_threads = 50
lock = threading.Lock()

threads = []
repos = []
layers = []

"""dest_dir contains three directories:
    0: root_dir
    1: manifest_dir
    2: config_dir
    3: layer_dir
"""
dest_dir = []

"""example https://registry-1.docker.io/v2/library/redis/manifests/latest; 
    note that we need to add library to official images"""
docker_io_http = "https://registry-1.docker.io/v2/"


def store_file(filename, resp):
    if type(resp.content == str):
        f = open(filename, 'w')
        f.write(resp.json())
    else:
        with open(filename, 'w') as fd:
            for chunk in resp.iter_content(chunk_size=128):
                fd.write(chunk)


def make_request(url):
    resp = requests.get(url)
    return resp


def download_manifest(repo):
    """download manifest first"""
    url = repo.docker_io_http + 'manifest' + '/' + repo.tag
    print 'manifest url: %s' % url
    resp = make_request(url)
    if not resp:
        return None
    else:
        """write to json file"""
        timestamp = time.time()
        filename = os.path.join(dest_dir['manifest_dir'], str(repo.name).replace("/", "-")+'-'+str(timestamp)+'.json')
        print filename
        store_file(filename, resp)
        return resp.json()


def download_blobs(repo, blobs_digest):
    """download image blob tar files"""
    digest_list = list(set(blobs_digest))  # remove redundant sha
    for digest in digest_list:
        if digest not in layers:
            layers.append(digest)
            url = repo.docker_io_http + 'blobs' + '/' + repo.tag
            print 'blobs url: %s' % url
            resp = make_request(url)
            timestamp = time.time()
            """write to tar file"""
            filename = os.path.join(dest_dir['layer_dir'], digest+'-'+str(timestamp))
            print filename
            store_file(filename, resp)


def download():
    while True:
        repo = q.get()
        if repo is None:
            break
        manifest = download_manifest(repo)
        if manifest is None:
            break
        blobs_digest = []
        if 'schemaVersion' in manifest and manifest['schemaVersion'] == 2:
            if 'config' in manifest and 'digest' in manifest['config']:
                config_digest = manifest['config']['digest']
                blobs_digest.append(config_digest)
            if 'layers' in manifest and isinstance(manifest['layers'], list) and len(manifest['layers']) > 0:
                for i in manifest['layers']:
                    if 'digest' in i:
                        print i['digest']
                        blobs_digest.append(i['digest'])

        elif 'schemaVersion' in manifest and manifest['schemaVersion'] == 1:
            if 'fsLayers' in manifest and isinstance(manifest['fsLayers'], list) and len(manifest['fsLayers']) > 0:
                for i in manifest['fsLayers']:
                    if 'blobSum' in i:
                        print i['blobSum']
                        blobs_digest.append(i['blobSum'])

        download_blobs(repo, blobs_digest)
        q.task_done()


def get_image_names(name):
    """process image file list and store the names in a file"""
    cmd1 = 'cp %s image-list.xls' % name
    cmd2 = 'awk -F\'' + ',' + '\' \'{print $3}\' image-list.xls > image-names.xls'
    print cmd1
    print cmd2
    rc = os.system(cmd1)
    assert (rc == 0)
    rc = os.system(cmd2)
    assert (rc == 0)


def is_official_repo(name):
    if str(name).count('/') > 0:
        return False
    else:
        return True


def construct_url(name, is_official):
    """official: add library in front"""
    if is_official:
        url = docker_io_http + 'library/' + name + '/'
        print "===============>"+url
        return url
    else:
        url = docker_io_http + name + '/'
        print "===============>"+url
        return url


def queue_names():
    with open('image-names.xls') as fd:
        for name in fd:
            if not name:
                continue
            repo = {
                'name': name,
                'is_official': is_official_repo(name),
                'docker_io_http': construct_url(name, is_official_repo(name)),
                'tag': 'latest'  # here we use latest as all images tags
            }
            repos.append(repo)

            print repo
            q.put(repo)


def load_layer_digests(dirname):
    for _, _, files in os.walk(dirname):
        for name in files:
            filename = os.path.join(dirname, name)
            if not os.path.isfile(filename):
                print ('%s is not a file', filename)
                continue
            else:
                if str(name).split('-')[0]:
                    print str(name).split('-')[0]
                    layers.append(str(name).split('-')[0])


def create_dirs(dirname):
    manifest_dir = os.path.join(dirname, "manifests")
    config_dir = os.path.join(dirname, "configs")
    layer_dir = os.path.join(dirname, "layers")
    if not os.path.exists(manifest_dir):
        os.makedirs(manifest_dir)
        print 'create manifest_dir: %s' % manifest_dir
    else:
        print 'manifest_dir: %s already exists' % manifest_dir
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
        print 'create config_dir: %s' % config_dir
    else:
        print 'config_dir: %s already exists' % config_dir
    if not os.path.exists(layer_dir):
        os.makedirs(layer_dir)
        print 'create layer_dir: %s' % layer_dir
    else:
        print 'layer_dir: %s already exists' % layer_dir
        load_layer_digests(layer_dir)

    dir = {
        'dirname': dirname,
        'manifest_dir': manifest_dir,
        'config_dir': config_dir,
        'layer_dir': layer_dir  # here we use latest as all images tags
    }
    dest_dir.append(dir)


def main():
    parser = OptionParser()
    parser.add_option('-f', '--filename', action='store', dest='filename',
                      help="The input file which contains all the images'names")
    parser.add_option('-d', '--dirname', action='store', dest='dirname',
                      help="The output directory which will contain three directories: manifests, configs, and layers")
    options, args = parser.parse_args()
    print 'Input file name: ', options.filename
    print 'Output directory: ', options.dirname

    if not os.path.isfile(options.filename):
        print '% is not a valid file' % options.filename
        return

    if not os.path.isdir(options.dirname):
        print '% is not a valid directory' % options.dirname
        return

    get_image_names(options.filename)
    create_dirs(options.dirname)

    queue_names()
    start = time.time()
    for i in range(num_worker_threads):
        t = threading.Thread(target=download)
        t.start()
        threads.append(t)

    q.join()
    print 'wait here!'
    for i in range(num_worker_threads):
        q.put(None)
    print 'put here!'
    for t in threads:
        t.join()
    print 'done here!'

    elapsed = time.time() - start
    print (elapsed / 3600)


if __name__ == '__main__':
    main()
    print 'should exit here!'
    exit(0)


