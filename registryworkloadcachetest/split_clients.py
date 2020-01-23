import json
import logging
from collections import defaultdict
from argparse import ArgumentParser

out = "/home/nannan/keren/clients/"


def split(traces):
    logging.info('splitting %d traces...' % len(traces))

    usr_map = defaultdict(list)
    #usr_list = list()
    i = 0

    for trace in traces:
        usr = trace['usr']
        if usr not in usr_map:
            usr_map[usr].append(trace)
            i += 1
        else:
            usr_map[usr].append(trace)
    
    logging.info("%d users discovered..." % len(usr_map))
    return usr_map
    #with open('test.lst', 'w') as f:
    #    json.dump(usr_list, f)
    #print(usr_map)
    #for usr in usr_map:
    #    print(usr_map[usr])



def distribute(n, usr_list):
    if len(usr_list) < n:
        n = len(usr_list)
    organized = [[] for x in range(n)]
    clientTOThreads = {}
    threadsize = {x: 0 for x in range(0,n)}

    i = 0
    for cli in sorted(usr_list, key=lambda k: len(usr_list[k]), reverse=True):
        #logging.debug(cli[0]['usr'] + ': ' + str(len(cli)))
        #print(cli)
        try:
            threadid = clientTOThreads[cli]
            organized[threadid].extend(usr_list[cli])
            threadsize[threadid] += len(usr_list[cli])
        except Exception as e:
            i += 1
            threadid = min(threadsize, key=threadsize.get)
            organized[threadid].extend(usr_list[cli])
            clientTOThreads[cli] = threadid
            threadsize[threadid] += len(usr_list[cli])
    #print organized[0][0:2]
    #print organized[1][0:2]
    #print organized[2][0:2]
    #print organized[3][0:2]

    for client in organized:
        #print(client)
        #print('\n\n\n')
        client.sort(key= lambda x: x['time'])
        base = client[0]['time']

        def normalize(x):
            x['time']-= base
            return x

        #print(client[0:2])
        client = map(normalize, client)
        #print(client[0:2])
    #print organized[0][0:2]
    #print organized[1][0:2]
    return organized


def get_info(usr_list):
    usr_avg = 0.0
    usr_max = 0
    usr_min = 114514
    for usr in usr_list:
        avg_span = 0.0
        max_span = 0
        min_span = 114514
        getm_trace = None
        getl_trace = None
        new = False
        up = False
        for trace in usr:
            if trace['M_T'] == 'GET l':
                if getl_trace == None:
                    getl_trace = trace
                    up = True
                elif getl_trace['time'] < getm_trace['time']:
                    getl_trace = trace
                    up = True
            elif trace['M_T'] == 'GET m':
                getm_trace = trace
                new = True

            if new and up:
                new = False
                up = False
                delta = getl_trace['time']-getm_trace['time']
                avg_span+=delta
                if delta>max_span:
                    max_span=delta
                if delta<min_span:
                    min_span=delta
        avg_span/=float(len(usr))
        usr_avg+=avg_span
        if max_span > usr_max:
            usr_max = max_span
        if min_span < usr_min:
            usr_min = min_span
        print("[ avg = %f, min = %d, max = %d ]" %(avg_span, min_span, max_span))
    print("usr max = %d\n usr min = %d\n usr avg = %f" %(usr_max, usr_min, usr_avg/len(usr_list)))


def main():

    logging.basicConfig(level=logging.DEBUG)
    logging.debug('started...')


    parser = ArgumentParser(description='Split into n clients.')
    parser.add_argument('-i', '--input', dest='input', type=str, required=True, 
                        help = 'Input json traces file')
    parser.add_argument('-n', '--nclients', dest='nclients', type=str, required=True,
                        help = 'Desired number of clients')
    args = parser.parse_args()

    logging.debug('reading input json...')
    with open(args.input, 'r') as f:
        traces = json.load(f)

    usr_list = split(traces)
    #get_info(usr_list.values())
    res = distribute(len(usr_list), usr_list)
    for i in range(len(res)):
        #logging.info('client %d has %d traces...' % (i, len(res[i])))
        with open(out+args.input+'client'+str(i)+'.json', 'w') as f:
            json.dump(res[i], f)


if __name__ == "__main__":
    main()
