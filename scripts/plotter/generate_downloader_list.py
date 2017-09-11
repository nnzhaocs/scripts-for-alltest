import os, json
from collections import defaultdict

with open('bad_layer_downloader_job_list.out') as f:
    all_jobs_json_data = json.load(f)

#bad_layer_downloader_job_list[image_mapper['bad_manifest']].append(digest) #= image_mapper['non_analyzed_layer_tarballs_digests']

downloader_list = []

with open('verification-FAILED.lst') as f:
    for line in f:
        print line
        if line:
            print "sha256:"+line.replace("\n", "")
            downloader_list.append("sha256:"+line.replace("\n", ""))

bad_layer_downloader_job_list = defaultdict(list)

for digest in downloader_list:
    finder = False
    for key, val in all_jobs_json_data.items():
        if digest in val:
            finder = True
            bad_layer_downloader_job_list[key].append(digest)
            print key
    if not finder:
        print digest


with open('bad_layer_downloader_job_list.out'), 'w+') as f_out:
    json.dump(bad_layer_downloader_job_list, f_out)