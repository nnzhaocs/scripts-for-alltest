import json
import datetime
import logging
import sys


limit = 15000
path = '/home/nannan/dockerimages/docker-traces/data_centers/'
jsons = ['dal-09_total_trace.json',
         'fra02_total_trace.json',
         'lon02_total_trace.json',
         'syd01_total_trace.json']
out = 'traces.lst'

def dowork(traces):
    #stats: total count, each req count, unique usr count, unique repo count
    usr_map = dict()
    gm = 0
    gl = 0
    pm = 0
    pl = 0
    mnfst_map = dict()

    res = list()
    req_map = dict() 
    lyr_map = dict()
    i = 0
    basetime = datetime.datetime.strptime(traces[0]['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
    curr_days = 0
    curr_seconds = 0
    curr_microseconds = 0
    
    for req in traces:
        if i >= limit:
            logging.info('reached desired trace count, leaving...')
            break


        #need: time, usr, repo, method+type, size, dgst
        if True:#try:
            uri = req['http.request.uri']
            """#type
            if 'blobs' in uri:
                t = 'l'
            elif 'manifests' in uri:
                t = 'm'
            else:
                continue"""
            #method
            r_type = req['http.request.method']
            

            #time
            timestamp = datetime.datetime.strptime(req['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')
            delta = timestamp - basetime
            """#size
            size = req['http.response.written']
            if t == 'm':
                size = 0"""
            #usr
            usr = req['http.request.remoteaddr']

            parts = uri.split('/')
            #repo
            repo = parts[1]+'/'+parts[2]
            #dgst
            dgst = uri.rsplit('/', 1)[1]

            #type
            if 'blobs' in uri:
                t = 'l'
                try:
                    lyr_map[dgst] += 1
                except:
                    lyr_map[dgst] = 1

            elif 'manifests' in uri:
                t = 'm'
                try:
                    mnfst_map[dgst] += 1
                except:
                    mnfst_map[dgst] = 1
            else:
                continue
            #size
            size = req['http.response.written']
            if t == 'm':
                size = 0

            if r_type == 'GET' and t == 'm':
                try:
                    x = req_map[usr]
                    if repo in x:
                        pass
                    else:
                        req_map[usr].append(repo)
                except:
                    req_map[usr] = []
                    req_map[usr].append(repo)
            elif r_type == 'GET' and t == 'l':
                try:
                    x = req_map[usr]
                    if repo not in x:
                        continue
                except:
                    continue


        '''except:
            e = sys.exc_info()[0]
            logging.debug(req)
            logging.debug(e)
            continue'''

        #stats
        if r_type == 'GET' and t == 'm':
            gm+=1
        if r_type == 'GET' and t == 'l':
            gl+=1
        if r_type == 'PUT' and t == 'm':
            pm+=1
        if r_type == 'PUT' and t == 'l':
            pl+=1
        try:
            usr_map[usr]+=1
        except:
            usr_map[usr] = 1

        if t != 'm':
            curr_days+=delta.days
            curr_seconds+=delta.seconds
            curr_microseconds+=delta.microseconds
            if curr_microseconds >= 1000000:
                curr_microseconds-=1000000
                curr_seconds+=1
            if curr_seconds >= 3600*24:
                curr_seconds-=3600*24
                curr_days+=1
        basetime = timestamp
            
        #print req
        res.append({'date' : curr_days, #delta.days,
                    'time' : curr_seconds, #delta.seconds,
                    'micro_time' : curr_microseconds, #delta.microseconds,
                   'usr' : usr,
                   'repo' : repo,
                   'M_T' : r_type + ' ' + t,
                   'size' : size,
                   'dgst' : dgst})

        i+=1
    logging.debug('dowork finished...')

    logging.info('total length = %d' % len(res))
    logging.info('unique user count = %d' % len(usr_map))
    logging.info('unique manifest count = %d' % len(mnfst_map))
    logging.info('unique layer count = %d' % len(lyr_map))
    logging.info('unique layer count = %d' % len(req_map))
    logging.info('get manifest count = %d' % gm)
    logging.info('get layer count = %d' % gl)
    logging.info('put manifest count = %d' % pm)
    logging.info('put layer count = %d' % pl)

    return res


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.debug('started...')

    for fn in jsons:
        with open(path + fn, 'r') as f:
            logging.debug('reading file...%s' % fn)
            traces = json.load(f)
        #logging.debug('#traces: %d which: %s' % len(traces), fn)
        res = dowork(traces)

        with open(fn+out, 'w') as f:
            json.dump(res, f)
        logging.debug('%s done...' % fn)

    logging.debug('finished...')


if __name__ == "__main__":
    main()
