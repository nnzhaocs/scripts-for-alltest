import sys
import json
import logging
from collections import defaultdict
from argparse import ArgumentParser
from siftcachenew2 import siftcache, extract
import siftcache_lru
from prefetch import prefetch_cache
from lru import LRU
import numpy as np
from split_clients import *
########################################
layer_buffer = LRU(15000)
URLmap = defaultdict(lambda: defaultdict(set))
RLmap = defaultdict(lambda: defaultdict(set))
build_time=7.95


def clear_buffer():
    global layer_buffer
    global URLmap
    layer_buffer.clear()
    URLmap = defaultdict(lambda: defaultdict(set))

def update_URLmap(request):
    client = request['usr']
    repo = request['repo']
    #newlogic==if request['method'] == 'PUT':
    layer = request['dgst']
#         else:
#             layer = request['getid']
    try:
        #if layer not in self.URLmap[client][repo]:
        URLmap[client][repo].add(layer)
    except KeyError:
        try:
            URLmap[client][repo] = {layer}
        except KeyError:
            URLmap[client] = {repo: {layer}}



def update_RLmap(request):
    client = request['usr']
    repo = request['repo']
    #newlogic==if request['method'] == 'PUT':
    layer = request['dgst']
#         else:
#             layer = request['getid']
    try:
        RLmap[repo][layer] = request['size']
    except KeyError:
        RLmap[repo] = {layer: request['size']}



def prefetch_layers(request):
    #print 'entered prefetch layers'
    client = request['usr']
    repo = request['repo']
    #print ' req ' + str(request)

    #newlogic==
    try:
        ph = URLmap[client][repo]
    except KeyError:
        try:
            URLmap[client][repo] = set()
        except KeyError:
            URLmap[client] = {repo:set()}

        #end of newlogic==

        #print(self.RLmap)
        #print(self.URLmap)

        '''print 'RLMAP ' + str(self.RLmap[repo])
        print 'URLMAP' + str(self.URLmap[client])
        #print ' req ' + str(request)
        client_layers = list(self.URLmap[client][repo])
        print 'layers ' + str(client_layers)'''
    try:
        repo_layers = RLmap[repo].keys()
    except KeyError:
        #print'key error'
        RLmap[repo] = {}
        repo_layers = RLmap[repo].keys()

    #print 'RLMAP ' + str(RLmap[repo])
    #print 'URLMAP' + str(URLmap[client])
    #print ' req ' + str(request)
    client_layers = list(URLmap[client][repo])
    #print 'layers ' + str(client_layers)


    prefetchable = list(set(repo_layers).difference(set(client_layers)))
    #print 'prefetchable: ' + str(prefetchable)
    #print 'self buffer: ' + str(self.layer_buffer)
    #to_prefetch = list(set(prefetchable).difference(set(self.layer_buffer.keys())))
    to_prefetch_excluding_putbuffer = list(set(prefetchable).difference(layer_buffer.keys()))
    #nannan
    #recent_prefetched_slicelayers += len(to_prefetch_excluding_putbuffer)
#         self.sediments += len(set(self.prefetched_layers_buffer.keys()))
    #size = 0
    for layer in prefetchable:
        #size += RLmap[repo][layer]
        #if self.layer_buffer.has_key(layer):
        #    del self.layer_buffer[layer]
        if layer_buffer.has_key(layer) and layer_buffer[layer]['time'] != -600:
            #layer_buffer[layer] = {'time': layer_buffer[layer],
            #                                    'size': RLmap[repo][layer],
            #                                    }
            layer_buffer[layer]
        else:
            layer_buffer[layer] = {'time': request['time'],
                                                'micro_time': request['micro_time'],
                                                'size': RLmap[repo][layer],
                                                'hits' : 0,
                                                }

    size = sum( [RLmap[repo][lyr] for lyr in to_prefetch_excluding_putbuffer] )
    return size

###########

def replay_lru(cache_sz, repodict, traces):
    #cache = LRU(len(repodict))#max(int(len(repodict)*0.2), 1))
    global layer_buffer
    #cache = layer_buffer
    hits = 4

    hit = 0
    wait = 0
    wait_t = float(0)
    miss = 0


    for req in traces:
        #print(req)
        m_t = req['M_T']
        size = 0
        if m_t == 'GET l':
            #status = 'miss'
            if layer_buffer.has_key(req['dgst']):
                layer_buffer[req['dgst']]
                #date_delta = req['time'] - layer_buffer[req['dgst']]['time']
                delta = req['time'] - layer_buffer[req['dgst']]['time']
                mic_delta = (req['micro_time'] - layer_buffer[req['dgst']]['micro_time'])/float(1000*1000)
                #print(mic_delta)
                #print(delta)
                #print(req)
                #print(layer_buffer[req['dgst']])

                if delta+mic_delta >= build_time:# and layer_buffer[req['dgst']]['hits'] >= hits:
                    hit+=1
                else:
                    wait+=1
                    #print(mic_delta)
                    #print(delta)
                    #print(req)
                    #print(layer_buffer[req['dgst']])
                    
                    wait_t+=(build_time - (delta+mic_delta))#abs(req['micro_time'] - layer_buffer[req['dgst']]['micro_time'])/float(1000*1000)
                update_URLmap(req)
                layer_buffer[req['dgst']]['hits']+=1
            else:
                #print(req)
                #print(URLmap[req['usr'][req[['repo']]])
                #print(RLmap[req['repo']])
                miss+=1
                update_URLmap(req)
                update_RLmap(req)
                size = prefetch_layers(req)#req['size']
                '''while size + curr_size > cache_sz:
                    #print(size)
                    lru_lyr = cache.peek_last_item()
                    #print(lru_lyr)
                    #print(cache)
                    if lru_lyr is None:
                        break
                    curr_size-=lru_lyr[1]
                    del cache[lru_lyr[0]]
                    
                cache[req['dgst']] = req['size']
                curr_size+=req['size']'''

        elif m_t == 'PUT l':
            layer_buffer[req['dgst']] =  {'time': req['time'],
                                                'micro_time': req['micro_time'],
                                                'size': req['size'],
                                                'hits' : 0,
                                                }
            #req['size']
            size = req['size']
            update_URLmap(req)
            update_RLmap(req)

            """curr_size+=req['size']
            '''while sum(cache.values()) > cache_sz:
            evict = cache.peek_last_item()
            del cache[evict[0]]'''
            while curr_size > cache_sz:
                lru_lyr = cache.peek_last_item()
                #print(str(lru_lyr) + str(lru_lyr[0]))
                curr_size-=lru_lyr[1]
                del cache[lru_lyr[0]]"""
        elif m_t == 'GET m':
                size = prefetch_layers(req)
        #curr_size = sum(lyr[1]['size'] for lyr in layer_buffer.items() if (lyr[1]['time']+build_time > req['time']))
        #print(curr_size)
        curr_size = 0
        keys = layer_buffer.keys()
        for i in reversed(range(0, len(keys))):
            lyr = layer_buffer[keys[i]]
            if lyr['time']+build_time < req['time']:# and lyr['hits'] >= hits:
                curr_size+=lyr['size']
        #print(curr_size)

        while curr_size > cache_sz:
            #print(curr_size)
            
            #print(curr_size > cache_sz)
            lru_lyr = layer_buffer.peek_last_item()
            #print(lru_lyr)
            #print(cache)
            if lru_lyr is None:
                break
            if lru_lyr[1]['hits'] >= hits:
                layer_buffer[lru_lyr[0]]
                if lru_lyr != layer_buffer.peek_first_item():
                    continue
                else:
                    break
            #print lru_lyr[1]
            curr_size-=lru_lyr[1]['size']
            #exit(0)
            del layer_buffer[lru_lyr[0]]
        #curr_size+=size
    if hit+miss+wait == 0:
        return
    print(cache_sz)
    print('miss = %d' % miss)
    print('wait = %d' % wait)
    print('wait time = ' + str(wait_t))
    print('hit = %d' % hit)
    print('total wait = ' + str(wait_t+miss*build_time))
    print('average wait = ' +  str((wait_t+wait*build_time)/(miss+wait+hit)))
    print('total unique get layers %d = ' % len(repodict))
    #print('cache slots = %d' % cache.get_size())


def replay_parallel(cache_sz, repodict, clients):
    #cache = LRU(len(repodict))#max(int(len(repodict)*0.2), 1))
    global layer_buffer
    #cache = layer_buffer
    curr_size = 0

    hit = 0
    wait = 0
    wait_t = float(0)
    miss = 0

    i = 0
    #
    while i < 5400:
        end = True
        for j in range(0, len(clients)):#client in clients:
            #print(client)
            try:
                
                req = client[index[j]]
                
                end = False
                
                #print(req)
                m_t = req['M_T']
                size = 0
                if m_t == 'GET l':
                    #status = 'miss'
                    if layer_buffer.has_key(req['dgst']):
                        layer_buffer[req['dgst']]
                        #date_delta = req['time'] - layer_buffer[req['dgst']]['time']
                        delta = req['time'] - layer_buffer[req['dgst']]['time']
                        mic_delta = (req['micro_time'] - layer_buffer[req['dgst']]['micro_time'])/float(1000*1000)
                        print(mic_delta)
                        print(delta)
                        print(req)
                        print(layer_buffer[req['dgst']])

                        if delta+mic_delta >= build_time:
                            hit+=1
                        else:
                            wait+=1
                            print(mic_delta)
                            print(delta)
                            print(req)
                            print(layer_buffer[req['dgst']])

                            wait_t+=(build_time - (delta+mic_delta))#abs(req['micro_time'] - layer_buffer[req['dgst']]['micro_time'])/float(1000*1000)
                        update_URLmap(req)
                    else:
                        miss+=1
                        update_URLmap(req)
                        update_RLmap(req)
                        size = prefetch_layers(req)#req['size']
                elif m_t == 'PUT l':
                    layer_buffer[req['dgst']] =  {'time': req['time'],
                                                'micro_time': req['micro_time'],
                                                'size': req['size'],
                                                }
                    #req['size']
                    size = req['size']
                    update_URLmap(req)
                    update_RLmap(req)

                elif m_t == 'GET m':
                        size = prefetch_layers(req)
                curr_size = sum(lyr[1]['size'] for lyr in layer_buffer.items())
                while curr_size > cache_sz:
                    #print(size)
                    lru_lyr = layer_buffer.peek_last_item()
                    #print(lru_lyr)
                    #print(cache)
                    if lru_lyr is None:
                        break
                    #print lru_lyr[1]
                    curr_size-=lru_lyr[1]['size']
                    #exit(0)
                    del layer_buffer[lru_lyr[0]]
                #curr_size+=size
            except Exception:
                continue
        i+=1
        if end:
            break

    if hit+miss+wait == 0:
        return
    print(curr_size)
    print('miss = %d' % miss)
    print('wait = %d' % wait)
    print('wait time = ' + str(wait_t))
    print('hit = %d' % hit)
    print('total wait = ' + str(wait_t+miss*build_time))
    print('average wait = ' +  str((wait_t+wait*build_time)/(miss+wait+hit)))
    print('total unique get layers %d = ' % len(repodict))
    #print('cache slots = %d' % cache.get_size())



#========================================
"""def replay_sift(cache_sz, repodict, traces):
    cache = LRU(int(len(repodict)*0.2))
    layer_buffer = {}
    layer_buffer_space_usage = []
    layer_buffer_space = 0
    prefetched_layers_buffer = {}
    prefetched_layers_buffer_space_usage = []
    prefetched_layers_buffer_space = 0
    total_evictions = 0
    threshold = cache_sz

    prefetch_layers_hit = 0
    #         self.prefetch_layers_miss = 0
    ##  layer buffer
    layer_buffer_hit = 0
    #         self.layer_buffer_miss = 0
        
    ###nannan: 
    #         self.pecentage
    recent_prefetched_slicelayers = 0
    recent_buffered_putlayers = 0
    sediments = 0

    hit = 0
    miss = 0

    def put(request):
        return False"""
#========================================


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.debug('started...')

    parser = ArgumentParser(description='Trace Player, allows for anonymized traces to be replayed to a registry, or for caching and prefecting simulations.')
    parser.add_argument('-i', '--input', dest='input', type=str, required=True, help = 'Input traces json file')
    parser.add_argument('-c', '--client', dest='client', type=int, required=True, help = 'Input traces json file')
    parser.add_argument('-p', '--percent', dest='percent', type=float, required=True, help = 'Input traces json file')
    args = parser.parse_args()
    logging.info('input trace file %s' % args.input)

    #get total size
    with open(args.input, 'r') as f:
        traces = json.load(f)
        logging.info('total length = %d' % len(traces))
    
    #set 20% cache
    max_repo = 0
    tot_sz = 0
    repodict = dict()
    #getMchecker = defaultdict(list)
    cnt = 0

    layerlst = list()
    putllst = list()

    for req in traces:
        '''if req['M_T'] == 'GET m' and (req['repo'] not in getMchecker or req['usr'] not in getMchecker[req['repo']]):
            #if len(getMchecker[req['repo']]) == 0:
            #    getMchecker[req['repo']] = []
            getMchecker[req['repo']].append(req['usr'])
        elif req['M_T'] == 'GET l' and (req['repo'] not in getMchecker or req['usr'] not in getMchecker[req['repo']]):
            print('WARNING!!!')
            cnt+=1'''
        if req['M_T'] == 'GET l':
            cnt+=1
        if req['M_T'] == 'GET l' and req['dgst'] not in layerlst:
            #print(type(req))
            
            layerlst.append(req['dgst'])
            '''rqst1 = req.copy()
            rqst1['M_T'] = 'PUT m'
            rqst1['size'] = 0
            #putllst.append(rqst1)'''
            rqst2 = req.copy()
            rqst2['M_T'] = 'PUT l'
            #rqst2['usr'] = 'default'
            rqst2['time'] = -600
            putllst.append(rqst2)
            #putllst.append(rqst1)
        if req['dgst'] not in repodict and 'l' in req['M_T']:
            tot_sz+=req['size']
            repodict[req['dgst']] = req['size']
            if max_repo < req['size']:
                max_repo = req['size']
    print(len(layerlst))
    print(len(putllst))
    print('cnt = ' + str(cnt))
    logging.info('max size = %d' % max_repo)
    logging.info('total unique size = %d' % tot_sz)
    #print(float(args.percent))
    cache_sz = int(max(args.percent * tot_sz, 1))
    print(cache_sz)
    print(tot_sz)
    print('50 percent =  %d' % np.percentile(repodict.values(), 50))
    print('75 percent =  %d' % np.percentile(repodict.values(), 75))
    print('90 percent =  %d' % np.percentile(repodict.values(), 90))
    print('95 percent =  %d' % np.percentile(repodict.values(), 95))
    print('99 percent =  %d' % np.percentile(repodict.values(), 99))
    #exit(0)
    #replay
    '''replay_lru(cache_sz, repodict, traces)
    prefetchhour10 = prefetch_cache(cache_size=cache_sz, rtimeout=3600, mtimeout=600)#, size_limit=cache_sz)
    #prefetchhour10.init(traces)
    for request in putllst:
        #print(request['repo'])
        prefetchhour10.put(request)
    prefetchhour10.clear()
    for request in traces:
        prefetchhour10.put(request)
    prefetchhour10.flush()
    data = [prefetchhour10.get_info()]
    outfile = "prefetch_trace.txt"
    f1 = open(outfile, 'a')
    f2 = open("prefetch_trace_detail.txt", 'a')
    for n in data:
        f2.write(str(n) + '\n') 
        size = n['max size']
        ratio = 1.*n['hits'] / (n['good prefetch'] + n['bad prefetch'])
        f1.write(str(ratio) + ',' + str(size) + '\n')
    f1.close()
    f2.close()

    '''
    usr_list = split(traces)
    #get_info(usr_list.values())
    res = distribute(int(args.client), usr_list)
    #sifthour10 = siftcache(600, cache_sz)
    requests = extract(traces)
    replay_lru(tot_sz, repodict, putllst)
    clear_buffer()
    all_traces = []
    for client in res:
        all_traces.extend(client)
    #print all_traces
    traces = sorted(all_traces, key = lambda req:(req['time'],req['micro_time']))
    
    #replay_parallel(cache_sz, repodict, res)
    replay_lru(cache_sz, repodict, traces)
    i = 0
    j = 0
    l = len(requests)
    count = 0
    
    '''setup_reqs = extract(putllst)
    for request in setup_reqs:
        sifthour10.put(request)
    sifthour10.clear_buffer()
    
    print "start putting data in cache"
    for request in requests:
        if 1.*i / l > 0.1:
            count += 10
            i = 0
            print str(count) + '% done'
        i += 1
        j += 1
        sifthour10.put(request)
    print(sifthour10.get_info())'''
    


if __name__ == "__main__":
    main()
