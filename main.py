
from imports import *
from images import *
from layers import *
from dir import *
from file import *
from draw_pic import *
from utility import *


# repos = []
# layers = []


def parseArg():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--debug',
        help="Print lots of debugging statements",
        action="store_const", dest="loglevel", const=logging.DEBUG,
        default=logging.INFO,
    )
    # parser.add_argument(
    #     '-v', '--verbose',
    #     help="Be verbose",
    #     action="store_const", dest="loglevel", const=logging.INFO,
    # )

    parser.add_argument(
        '-c', '--create',
        help="Create layer database",
        action="store_true"
    )

    """ dest_dir contains three directories:
        0: root_dir
        1: manifest_dir
        2: config_dir
        3: layer_dir
        manifest_dir = os.path.join(dest_dir, "manifests")
        config_dir = os.path.join(dest_dir, "configs")
        layer_dir = os.path.join(dest_dir, "layers")
    """
    parser.add_argument(
        '-t', '--tarball',
        help="Get tarball and extract it",
        action="store_true", dest="dest_dir", const=logging.INFO,
    )

    return parser.parse_args()


def main():
    args = parseArg()

    logging.basicConfig(level=args.loglevel)

    manifest_dir = os.path.join(args.dest_dir, "manifests")
    config_dir = os.path.join(args.dest_dir, "configs")
    layer_dir = os.path.join(args.dest_dir, "layers")

    dir = {
        'dirname': args.dest_dir,
        'manifest_dir': manifest_dir,
        'config_dir': config_dir,
        'layer_dir': layer_dir
    }

    dest_dir.append(dir)

    start = time.time()
    if not args.tarball:
        if args.create:
            logging.info('The output layer_db_filename: %s', layer_db_filename)
            f_layer_db = open(layer_db_filename, 'w+')
            queue_layers()

            for i in range(num_worker_threads):
                t = threading.Thread(target=load_layer, args=(f_layer_db,))
                t.start()
                threads.append(t)

            q.join()
            logging.info('wait queue to join!')
            for i in range(num_worker_threads):
                q.put(None)
            logging.info('put none layers to queue!')
            for t in threads:
                t.join()
            logging.info('done! all the threads are finished')

            elapsed = time.time() - start
            logging.info('create layer json file, consumed time ==> ' % (elapsed / 3600))

            # images = load_images()
            # cal_layer_repeats(images)
            # with open(db_filename, 'w+') as f_out:
            #     json.dump(images, f_out)

            # print "here loaded images to file: database_json!"
        # else:
        #     logging.info('loading images ...')
        #     with open(db_filename, 'r') as f_out:
        #         images = json.load(f_out)

            # logging.info('analyzing/plotting images/layers ...')

    # elapsed = time.time() - start
    # print (elapsed / 3600)
        #plt_repeat_layer(images)
        #plt_files_size(images)

if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
