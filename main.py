
from imports import *
# from images import *
from layers import *
from dir import *
from file import *
from draw_pic import *
from utility import *


def parseArg():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-d', '--debug',
        help="Print lots of debugging statements",
        action="store_const", dest="loglevel", const=logging.DEBUG,
        default=logging.INFO,
    )

    parser.add_argument(
        '-v', '--verbose',
        help="Be verbose",
        action="store_const", dest="loglevel", const=logging.INFO,
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
        '-d', '--directory',
        help="Directory which contains manfiest and tarballs",
        action="store_true", dest="dest_dir",
    )

    parser.add_argument(
        '-c', '--create',
        help="Create layer database",
        action="store_true"
    )

    # parser.add_argument(
    #     '-i', '--create',
    #     help="Create layer database",
    #     action="store_true"
    # )

    return parser.parse_args()


def main():
    args = parseArg()

    logging.basicConfig(level=args.loglevel)

    start = time.time()
    if args.dest_dir and args.create:
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
        logging.info('dir list is: %s', dir)
        f_layer_db = open(layer_db_filename, 'w+')
        create_layer_db(f_layer_db)
        f_layer_db.close()



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

    elapsed = time.time() - start
    logging.info('create layer json file, consumed time ==> ' % (elapsed / 3600))

        #plt_repeat_layer(images)
        #plt_files_size(images)

if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
