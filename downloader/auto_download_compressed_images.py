import os, sys, subprocess, select, random, urllib2, time, json, tempfile, shutil
import re
import threading, Queue
import argparse
from optparse import OptionParser
#import requests
import subprocess

q = Queue.Queue()
layers_q = Queue.Queue()

num_worker_threads = 6
num_layer_worker_threads = 6
lock = threading.Lock()

threads = []
repos = []

# /* TODO
# 	1. add log file (store all the errors)
#
# 	2. catch exceptions (input, timeout)
# */

"""dest_dir contains three directories:
    0: root_dir
    1: manifest_dir
    2: config_dir
    3: layer_dir
    manifest_dir = os.path.join(dest_dir, "manifests")
    config_dir = os.path.join(dest_dir, "configs")
    layer_dir = os.path.join(dest_dir, "layers")
"""
dest_dir = []

# """example https://registry-1.docker.io/v2/library/redis/manifests/latest;
#     note that we need to add library to official images"""
# docker_io_http = "https://registry-1.docker.io/v2/"


# def store_file(filename, resp):
#     if type(resp.content == str):
#         f = open(filename, 'w')
#         f.write(resp.json())
#     else:
#         with open(filename, 'w') as fd:
#             for chunk in resp.iter_content(chunk_size=128):
#                 fd.write(chunk)


def make_request(req):
    """send request to docker.io. call golang"""
    """go run down_loader.go -operation=download_blobs -repo=library/redis -tag=44888ef5307528d97578efd747ff6a5635facbcfe23c84e79159c0630daf16de  -absfilename=./test
        go run down_loader.go -operation=download_manifest -repo=library/redis -tag=latest -absfilename=./test"""

    args = "go run down_loader.go -operation=%s -repo=%s -tag=%s -absfilename=%s" % (req['operation'], req['repo_name'], req['repo_tag'], req['absfilename'])
    try:
        subprocess.check_output(args, shell=True)
    except subprocess.CalledProcessError as e:
        print e.output
    # resp, err = p.communicate()
    # return resp


def download_manifest(repo):
    """download manifest first"""
    # url = repo['docker_io_http'] + 'manifest' + '/' + repo['tag']
    # print 'manifest url: %s' % url
    timestamp = time.time()
    filename = os.path.join(dest_dir[0]['manifest_dir'], str(repo['name']).replace("/", "-") + '-' + repo['tag'] + '-' + str(timestamp) + '.json')
    print filename
    req = {
        'repo_name': repo['name'],
        'repo_tag': repo['tag'],
        'operation': 'download_manifest',
        'absfilename': filename
    }
    make_request(req)
    if not os.path.isfile(filename):
        return None
    with open(filename) as manifest_file:
        resp = json.load(manifest_file)
    if not resp:
        return None
    else:
        """return json"""
        print resp
        # sstr = str(resp).split('<manifest>')
        # if sstr:
        #     print sstr
        #     manifest = sstr[1]
        #     # return resp.communicate()
        #     print manifest
        #     # return manifest
        #     return json.loads(str(manifest))
        # else:
        return resp
        # d = json.loads()
        # filename = os.path.join(dest_dir['manifest_dir'], str(repo.name).replace("/", "-")+'-'+str(timestamp)+'.json')
        # store_file(filename, resp)
        # return resp.json()


def download_blobs(repo, blobs_digest):
    """download image blob tar files"""
    # digest_list = list(set(blobs_digest))  # remove redundant sha
    while True:
        digest = blobs_digest.get()
        if digest is None:
            break
        with lock:
            if digest in layers_q.queue:
                print "Layer Already Exist!"
                is_layer_exist = True
            else:
                is_layer_exist = False
                layers_q.put(digest)  # queue
                print "Layer Not Exist!"
        # url = repo.docker_io_http + 'blobs' + '/' + repo.tag
        # print 'blobs url: %s' % url
        if not is_layer_exist:
            timestamp = time.time()
            filename = os.path.join(dest_dir[0]['layer_dir'], str(digest).replace(':', '-') + '-' + str(timestamp))
            print filename
            str_digest = str(digest).split('sha256:')
            # print str_digest
            if str_digest[1]:
                print str_digest[1]
                req = {
                    'repo_name': repo['name'],
                    'repo_tag': str_digest[1],
                    'operation': 'download_blobs',
                    'absfilename': filename
                }
                make_request(req)
        blobs_digest.task_done()
    # """write to tar file"""
    # store_file(filename, resp)


def manifest_schemalist(manifest):
    blobs_digest = []
    if 'manifests' in manifest and isinstance(manifest['manifests'], list) and len(manifest['manifests']) > 0:
        for i in manifest['manifests']:
            if 'digest' in i:
                print i['digest']
                blobs_digest.append(i['digest'])
    return blobs_digest


def manifest_schema2(manifest):
    blobs_digest = []
    if 'config' in manifest and 'digest' in manifest['config']:
        config_digest = manifest['config']['digest']
        blobs_digest.append(config_digest)
    if 'layers' in manifest and isinstance(manifest['layers'], list) and len(manifest['layers']) > 0:
        for i in manifest['layers']:
            if 'digest' in i:
                print i['digest']
                blobs_digest.append(i['digest'])
    return blobs_digest


def manifest_schema1(manifest):
    blobs_digest = []
    if 'fsLayers' in manifest and isinstance(manifest['fsLayers'], list) and len(manifest['fsLayers']) > 0:
        for i in manifest['fsLayers']:
            if 'blobSum' in i:
                print i['blobSum']
                blobs_digest.append(i['blobSum'])
    return blobs_digest


def download():
    while True:
        repo = q.get()
        if repo is None:
            break
        manifest = download_manifest(repo)
        if manifest is None:
            continue
        blobs_digest = []
        layer_threads = []
        if 'schemaVersion' in manifest and manifest['schemaVersion'] == 2:
            if 'mediaType' in manifest and 'list' in manifest['mediaType']:
                blobs_digest = manifest_schemalist(manifest)
            else:
                blobs_digest = manifest_schema2(manifest)
            # if 'config' in manifest and 'digest' in manifest['config']:
            #     config_digest = manifest['config']['digest']
            #     blobs_digest.append(config_digest)
            # if 'layers' in manifest and isinstance(manifest['layers'], list) and len(manifest['layers']) > 0:
            #     for i in manifest['layers']:
            #         if 'digest' in i:
            #             print i['digest']
            #             blobs_digest.append(i['digest'])

        elif 'schemaVersion' in manifest and manifest['schemaVersion'] == 1:
            blobs_digest = manifest_schema1(manifest)
            # if 'fsLayers' in manifest and isinstance(manifest['fsLayers'], list) and len(manifest['fsLayers']) > 0:
            #     for i in manifest['fsLayers']:
            #         if 'blobSum' in i:
            #             print i['blobSum']
            #             blobs_digest.append(i['blobSum'])

        # download_blobs(repo, blobs_digest)
        digest_list = list(set(blobs_digest))  # remove redundant sha
        blobs_digest_q = Queue.Queue()
        for i in digest_list:
            blobs_digest_q.put(i)
        for i in range(num_layer_worker_threads):
            t = threading.Thread(target=download_blobs, args=(repo, blobs_digest_q))
            t.start()
            layer_threads.append(t)

        blobs_digest_q.join()
        print 'wait here!'
        for i in range(num_layer_worker_threads):
            blobs_digest_q.put(None)
        print 'put here!'
        for t in layer_threads:
            t.join()
        print 'done here!'

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


# def is_official_repo(name):
#     if str(name).count('/') > 0:
#         return False
#     else:
#         return True


# def construct_url(name, is_official):
#     """official: add library in front"""
#     if is_official:
#         url = docker_io_http + 'library/' + name + '/'
#         print "===============>"+url
#         return url
#     else:
#         url = docker_io_http + name + '/'
#         print "===============>"+url
#         return url


def queue_names():
    with open('image-names.xls') as fd:
        for name1 in fd:
            if not name1:
                continue
            name = str(name1).replace(" ", "").replace("\n", "")
            if name is None:
                continue
            repo = {
                'name': name,
                # 'is_official': is_official_repo(name),
                # 'docker_io_http': construct_url(name, is_official_repo(name)),
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
                sstr = str(name).split('-')
                if sstr:
                    print sstr
                    digest = sstr[0]+':'+sstr[1]  # digest with sha256
                    layers_q.put(digest)


def create_dirs(dirname):
    manifest_dir = os.path.join(dirname, "manifests")
    config_dir = os.path.join(dirname, "configs")
    layer_dir = os.path.join(dirname, "layers")
    # if not os.path.exists(manifest_dir):
    #     os.makedirs(manifest_dir)
    #     print 'create manifest_dir: %s' % manifest_dir
    # else:
    #     print 'manifest_dir: %s already exists' % manifest_dir
    """Here, we create new manifest dir if manifest exist! and mv old manifest to manifest-timestamp"""
    if os.path.exists(manifest_dir):
        timestamp = time.time()
        cmd5 = "mv %s %s" % (manifest_dir, os.path.join(dirname, "manifests"+str(timestamp)))
        rc = os.system(cmd5)
        assert (rc == 0)
    os.makedirs(manifest_dir)
    print 'create manifest_dir: %s' % manifest_dir
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


