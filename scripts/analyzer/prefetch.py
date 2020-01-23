'''repo_r = 0
client_l=0
repo_m = 0
repo_im = 0
layer_f = 0
layer_nf = 0
tot = 0'''
class prefetch_cache:

    def __init__(self, cache_size, rtimeout=600, mtimeout=600):
        self.repos = {}
        self.manifest = {}
        self.rtimeout = rtimeout
        self.mtimeout = mtimeout
        self.hit = 0
        self.miss = 0
        self.size = 0
        self.size_list = []
        self.goodprefetch = 0
        self.badprefetch = 0
        self.putcount = 0
        self.getlayercount = 0
        self.getmanifestcount = 0
        self.cache_size = cache_size
        '''self.dgst_list = []
        self.weird = 0'''
        '''for request in repos:
            repo = request['repo']
            client = request['client']
            timestamp = request['timestamp']
            size = request['size']
            self.update_repos(repo, client, size, timestamp)'''
    def clear(self):
        self.manifest = {}


    def flush(self):
        for repo in self.manifest:
            for layer in self.manifest[repo]:
                count = layer[1]
                self.size -= layer[2]
                self.size_list.append(self.size)
                if count > 0:
                    self.goodprefetch += 1
                else:
                    self.badprefetch += 1
        self.manifest = {}

    def update_manifests(self, repo, client, timestamp):
        '''global tot
        global repo_r
        global client_l
        global repo_m
        global repo_im
        global layer_f
        global layer_nf
        tot+=1'''
        if repo in self.repos:
            #repo_r+=1
            #print("found")
            for layer in self.repos[repo]:
                if client not in layer[1]:
                    #client_l+=1
                    layer[1].append(client)
                    if repo not in self.manifest:
                        #repo_m+=1
                        self.manifest[repo] = [[timestamp, 0, layer[2]]]
                        self.size += layer[2]
                        self.size_list.append(self.size)
                    else:
                        #repo_im+=1
                        present = False
                        for fetchedlayer in self.manifest[repo]:
                            if fetchedlayer[2] == layer[2]:
                                #layer_f+=1
                                fetchedlayer[0] = timestamp
                                present = True
                                break
                        if present == False:
                            #layer_nf+=1
                            self.manifest[repo].append([timestamp, 0, layer[2]])
                            self.size += layer[2]
                            self.size_list.append(self.size)
        #else:
        #    #print(repo)
        #    #print(timestamp)
        #    #exit(0)


    def manifest_time_out(self, timestamp):
        remove = []
        for repo in self.manifest:
            i = 0
            while i < len(self.manifest[repo]):
                t = self.manifest[repo][i][0]
                count = self.manifest[repo][i][1]
                delta = timestamp - t
                if abs(delta) > self.mtimeout:
                    self.size -= self.manifest[repo][i][2]
                    self.size_list.append(self.size)
                    self.manifest[repo].pop(i)
                    if count > 0:
                        self.goodprefetch += 1
                    else:
                        self.badprefetch += 1
                else:
                    i += 1
            if len(self.manifest[repo]) == 0:
                remove.append(repo)
        for repo in remove:
            self.manifest.pop(repo, None)



    def repo_time_out(self, timestamp):
        remove = []
        for repo in self.repos:
            i = 0
            while i < len(self.repos[repo]):
                t = self.repos[repo][i][0]
                delta = timestamp - t
                if abs(delta) > self.rtimeout:
                    self.repos[repo].pop(i)
                else:
                    break
            if len(self.repos[repo]) == 0:
                remove.append(repo)
        for repo in remove:
            self.repos.pop(repo, None)

    def update_repos(self, repo, client, size, timestamp):
        if repo in self.repos:
            self.repos[repo].append([timestamp, [client], size])
        else:
            self.repos[repo] = [[timestamp, [client], size]]

    def update_layers(self, repo, size, timestmap):
        if repo in self.manifest:
            for layer in self.manifest[repo]:
                if size == layer[2]:# and size + 1000 > layer[2]:
                    self.hit += 1
                    layer[1] += 1
                    return #new logic
            '''print(repo)
            print(timestmap)
            print(size)
            exit(0)'''
            #self.miss+=1 #new logic
        else:
            #print(repo)
            #print(timestmap)
            #print(size)
            #exit(0)
            self.miss += 1
            #self.weird+=1
        
    def put(self, request):
        '''print(request)
        print(self.repos)
        print(self.manifest)'''
        repo = request['repo']
        client = request['usr']
        timestamp = request['time']
        size = request['size']
        count = 0
        while size+self.size > self.cache_size and len(self.manifest) > 0:
            self.manifest_time_out(timestamp+count)
            count+=1
            #if size+self.size > self.cache_size:
            #    print('beep')
        #self.repo_time_out(timestamp)
        '''###new logic####
        dgst = request['dgst']
        if dgst not in self.dgst_list:
            self.dgst_list.append(dgst)'''
        #self.update_repos(repo, client, size, timestamp)
        #self.update_manifests(self, repo, client, timestamp)
        if 'PUT' in request['M_T']:# == 'PUT':
            self.putcount += 1
            self.update_repos(repo, client, size, timestamp)
        elif 'm' in request['M_T']:# == 'm':
            '''# new logic
            if repo not in self.repos:
                self.repos[repo] = [[timestamp, [client], size]]'''
            #print('hi')
            self.getmanifestcount += 1
            self.update_manifests(repo, client, timestamp)
        else:
            '''# new logic
            if repo not in self.repos:
                self.repos[repo] = [[timestamp, [client], size]]'''
            self.getlayercount += 1
            self.update_layers(repo, size, timestamp)

    def get_info(self):
        '''print('weird'+str(self.weird))
        print("self.getlayercount: " + str(self.getlayercount))
        print("repo_r = " + str(repo_r))
        print('client_l=' + str(client_l))
        print('repo_m = ' + str(repo_m))
        print('repo_im = ' + str(repo_im))
        print('layer_f = ' + str(layer_f))
        print('layer_nf = ' + str(layer_nf))
        print('tot = ' + str(tot))'''
        print(self.hit)
        print(self.miss)
        print(self.getlayercount)
        print(self.putcount)
        print(self.getmanifestcount) 
        data = {
            'hits': self.hit,
            'misses': self.miss,
            'good prefetch': self.goodprefetch,
            'bad prefetch': self.badprefetch,
            'max size': max(self.size_list)}
        return data

    def get_size_list(self):
        return self.size_list

def extract(data):
  
    requests = []

    for request in data:
        method = request['method']

        if 'blobs' in request['uri']:
            t = 'l'
        elif 'manifests' in request['uri']:
            t = 'm'
        else:
            continue

        parts = request['uri'].split('/')
        repo = parts[1] + '/' + parts[2]
        requests.append({'timestamp': request['delay'], 
                        'repo': repo, 
                        'client': request['client'], 
                        'method': request['method'], 
                        'type': t,
                        'size': request['size']})
    return requests


def init(data, args):
    outfile = args['outfile']

    requests = extract(data)

    print 'running simulation'

    prefetch1010 = prefetch_cache(rtimeout=600, mtimeout=600)
    prefetch10hour = prefetch_cache(rtimeout=600, mtimeout=3600)
    prefetch10half = prefetch_cache(rtimeout=600, mtimeout=43200)
    prefetch10day = prefetch_cache(rtimeout=600, mtimeout=86400)
    prefetchhour10 = prefetch_cache(rtimeout=3600, mtimeout=600)
    prefetchhourhour = prefetch_cache(rtimeout=3600, mtimeout=3600)
    prefetchhourhalf = prefetch_cache(rtimeout=3600, mtimeout=43200)
    prefetchhourday = prefetch_cache(rtimeout=3600, mtimeout=86400)
    prefetchhalf10 = prefetch_cache(rtimeout=43200, mtimeout=600)
    prefetchhalfhour = prefetch_cache(rtimeout=43200, mtimeout=3600)
    prefetchhalfhalf = prefetch_cache(rtimeout=43200, mtimeout=43200)
    prefetchhalfday = prefetch_cache(rtimeout=43200, mtimeout=86400)
    prefetchday10 = prefetch_cache(rtimeout=86400, mtimeout=600)
    prefetchdayhour = prefetch_cache(rtimeout=86400, mtimeout=3600)
    prefetchdayhalf = prefetch_cache(rtimeout=86400, mtimeout=43200)
    prefetchdayday = prefetch_cache(rtimeout=86400, mtimeout=86400)
    i = 0
    l = len(requests)
    count = 0
    for request in requests:
        if 1.*i / l > 0.1:
            count += 10
            i = 0
            print str(count) + '% done'
        i += 1
        prefetch1010.put(request)
        prefetch10hour.put(request)
        prefetch10half.put(request)
        prefetch10day.put(request)
        prefetchhour10.put(request)
        prefetchhourhour.put(request)
        prefetchhourhalf.put(request)
        prefetchhourday.put(request)
        prefetchhalf10.put(request)
        prefetchhalfhour.put(request)
        prefetchhalfhalf.put(request)
        prefetchhalfday.put(request)
        prefetchday10.put(request)
        prefetchdayhour.put(request)
        prefetchdayhalf.put(request)
        prefetchdayday.put(request)
    prefetch1010.flush()
    prefetch10hour.flush()
    prefetch10half.flush()
    prefetch10day.flush()
    prefetchhour10.flush()
    prefetchhourhour.flush()
    prefetchhourhalf.flush()
    prefetchhourday.flush()
    prefetchhalf10.flush()
    prefetchhalfhour.flush()
    prefetchhalfhalf.flush()
    prefetchhalfday.flush()
    prefetchday10.flush()
    prefetchdayhour.flush()
    prefetchdayhalf.flush()
    prefetchdayday.flush()
    

    data = [prefetch1010.get_info(),
            prefetch10hour.get_info(),
            prefetch10half.get_info(),
            prefetch10day.get_info(),
            prefetchhour10.get_info(),
            prefetchhourhour.get_info(),
            prefetchhourhalf.get_info(),
            prefetchhourday.get_info(),
            prefetchhalf10.get_info(),
            prefetchhalfhour.get_info(),
            prefetchhalfhalf.get_info(),
            prefetchhalfday.get_info(),
            prefetchday10.get_info(),
            prefetchdayhour.get_info(),
            prefetchdayhalf.get_info(),
            prefetchdayday.get_info()]
    
    f = open(outfile, 'w')
    for n in data:
        size = n['max size']
        ratio = 1.*n['hits'] / (n['good prefetch'] + n['bad prefetch'])
        f.write(str(ratio) + ',' + str(size) + '\n')
    f.close()

