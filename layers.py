
from imports import *
from draw_pic import *
from utility import *
from dir import *
from itertools import chain

def load_layer(chainid):
    """ first we find the layer folder with chain id under /var/lib/docker/image/aufs/layerdb
    then the file 'cache-id' has the contents of the real layer id
    """
    cache_file = os.path.join(LAYER_STORE_DIR, 'sha256', chainid, 'cache-id')
    #print  os.path.join(LAYER_STORE_DIR, 'sha256', chainid, 'cache-id')
    if not os.path.isfile(cache_file):
        logging.info('no cache-id file found for this chain id: %s', chainid)
        return []

    with open(cache_file) as data_file:
        cache_id = data_file.read()

    if len(cache_id) == 0:
        logging.info('cache-id file is empty, no layers for this chain id:%s' % chainid)
        return []

    logging.debug('found cache id for chain id %s -> %s' % (chainid, cache_id))

    layer = {
        'chain_id': chainid,
        'cache_id': cache_id,
        'dirs': load_dirs(cache_id),  # getLayersBychainID(chain_id),
        'size': getLayersSize(chainid),
        'repeats': 0
    }

    #print layer
    return layer

def getLayersSize(chainid):
    """ first we find the layer folder with chain id under /var/lib/docker/image/aufs/layerdb
    then the file 'size' has size"""
    size_file = os.path.join(LAYER_STORE_DIR, 'sha256', chainid, 'size')
    #print  size_file
    if not os.path.isfile(size_file):
        logging.info('no size file found for this chain id: %s', chainid)
        return []

    with open(size_file) as data_file:
        size = data_file.read()

    if len(size) == 0:
        logging.info('size file is empty, no layers for this chain id:%s' % chainid)
        return []

    logging.debug('found size for chain id %s -> %s' % (chainid, size))
    return size

def cal_layer_repeats(images):
    """ [imge1's layers, image2's layers, ..... ]
    get unique elements among multiple lists: all_list"""
    fout = open('layer-repeats.txt', 'w+')
    layers = []
    for image in images:
        image_layers = []
        for layer in image['layers']:
            #print layer
            diff_path = os.path.join(AUFS_DIFF_DIR, layer['cache_id'])
            logging.debug('%s', '\n'.join(diff_path))
            """here, find all layer for each image"""
            image_layers.append(layer['cache_id'])
        layers.append(image_layers)
    print layers[0]

    layer_union = list(chain(*layers))
    print layer_union[0]
    layer_dict = {i:layer_union.count(i) for i in layer_union}
    #layer_repeats_dict = cal_layer_repeats(images)
    for k, v in layer_dict.items():
        print (k, v)
        fout.writelines(str(k)+','+str(v)+'\n')
        for image in images:
            for layer in image['layers']:
                #print layer
                if layer['cache_id'] == k:
                    layer['repeats'] = v
                #print layer

def plt_repeat_layer(images):
    d = {}
    for image in images:
        for layer in image['layers']:
            if layer['repeats'] not in d:
                d[layer['repeats']] = []
            d[layer['repeats']].append(layer['size'])
            #print d

    sort_layersbyrepeats = sorted(d.items())

    x = []
    y = []
    for item in sort_layersbyrepeats:
        print (item[0], item[1])
        k = item[0]
        v = item[1]
        sum1 = sum(map(int, v))
        x.append(int(k))
        y.append(float(sum1)/len(v) / 1024 / 1024)

    fig = fig_size('small')
    plot_bar_pic(fig, x, y, 'Repeats', 'Average Size(MB)', max(x), max(x))

    x = []
    y = []
    for item in sort_layersbyrepeats:
        print (item[0], item[1])
        k = item[0]
        v = item[1]
        sum1 = sum(map(int, v))
        x.append(int(k))
        y.append(float(sum1) / 1024 / 1024 / 1024)
    fig = fig_size('small')
    plot_bar_pic(fig, x, y, 'Repeats', 'Total Size(GB)', max(x), max(x))

    x = []
    y = []
    for item in sort_layersbyrepeats:
        print (item[0], item[1])
        k = item[0]
        v = item[1]
        # sum1 = sum(map(int, v))
        x.append(int(k))
        y.append(len(v))
    fig = fig_size('small')
    plot_bar_pic(fig, x, y, 'Repeats', 'file count', max(x), max(x))

#def plt_repeat_layer(images):
