
from imports import *
from draw_pic import *
from utility import *
from layers import *

def chainID(diffs):
    """ calculate chain-id from diff layers this is the same logical as in docker's source code"""
    if len(diffs) == 0:
        return ''

    def chain_hash(x, y):
        s = hashlib.sha256(bytearray(x + ' ' + y, 'utf8')).hexdigest()
        return 'sha256:' + s
    #print reduce(chain_hash, diffs)[7:]
    return reduce(chain_hash, diffs)[7:]

def load_images():
    """load all images from /var/lib/docker/image/aufs/imagedb/content/sha256
    files under this folder is the image spec json file, the filename is same as image id
    """
    images = []

    content_dir = os.path.join(IMAGE_STORE_DIR, 'content/sha256')
    for content_file in os.listdir(content_dir):
        logging.info('found image: %s' % content_file)
        with open(os.path.join(IMAGE_STORE_DIR, 'content/sha256', content_file)) as content:
            image_content = json.load(content)

        if 'rootfs' in image_content and 'diff_ids' in image_content['rootfs']:
            diffs = image_content['rootfs']['diff_ids']
            if isinstance(diffs, list) and len(diffs) > 0:
                chain_id = chainID(diffs)
                logging.debug('calcuated chain id from diff ids: %s' % chain_id)

                layers = []

                while chain_id:
                    chain_id = get_chain_ids(chain_id)
                    if chain_id:
                        layers.append(load_layer(chain_id))
                        print chain_id

                image = {
                    'content_id': content_file,
                    #'chain_id': chain_dis,
                    'layers': layers,
                    'layer_cnt': len(layers)
                }
                #print image
                images.append(image)
            else:
                logging.warn('image content diffs is empty or invalid, skip this image')
        else:
            logging.warn('could not find rootfs diffs in image content, skip this image')
    return images

def get_chain_ids(chain_id):
    parent_chainid_file = os.path.join(LAYER_STORE_DIR, 'sha256', chain_id, 'parent')
    print  parent_chainid_file
    if not os.path.isfile(parent_chainid_file):
        logging.info('no parent_chainid_file file found for this chain id: %s', chain_id)
        return None

    with open(parent_chainid_file) as data_file:
        temp = data_file.read()
        parent_chain_id = temp.replace("sha256:", "")

    if len(parent_chain_id) == 0:
        logging.info('cache-id file is empty, no layers for this chain id:%s' % chain_id)
        return None

    logging.debug('found parent_chain_id for chain id %s -> %s' % (chain_id, parent_chain_id))
    return parent_chain_id




















