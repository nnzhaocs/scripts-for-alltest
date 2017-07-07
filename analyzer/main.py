
from imports import *
# from images import *
from layers import *
from dir import *
from file import *
from draw_pic import *
from utility import *

""" TODO:
    1. put layer json to a single file
     2. fetch all the manifest
     3. get all the tags
"""
"""python main.py -D -d dest_dir -l analyzed_layer_file -e extracting_dir"""


def parseArg():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-D', '--debug',
        help="Print lots of debugging statements",
        action="store_const", dest="loglevel", const=logging.DEBUG,
        default=logging.INFO,
    )

    # parser.add_argument(
    #     '-v', '--verbose',
    #     help="Be verbose",
    #     action="store_const", dest="loglevel", const=logging.INFO,
    # )

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
        dest="dest_dir",
    )

    parser.add_argument(
        '-e', '--extracting_dir',
        help="Directory which is used to extracting tarball files",
        dest="extracting_dir",
    )

    # parser.add_argument(
    #     '-f', '--file',
    #     help="file which contains downloaded layers from downloader module, eg., finished_layer_list.out",
    #     dest="downloaded_layer_file",
    # )

    parser.add_argument(
        '-l', '--analyzed_layer_file',
        help="file which contains analyzed layers from downloader module, eg., finished_layer_list.out",
        dest="analyzed_layer_file",
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
    # if args.dest_dir:
    logging.info('Input dir name: %s', args.dest_dir)
    if not os.path.isdir(args.dest_dir):
        logging.error('%s is not a valid dir', args.dest_dir)
        return

    # manifest_dir = os.path.join(args.dest_dir, "manifests")
    # if not os.path.isdir(manifest_dir):
    #     logging.error('%s is not a valid dir', manifest_dir)
    #     return

    config_dir = os.path.join(args.dest_dir, "configs")
    if not os.path.isdir(config_dir):
        logging.error('%s is not a valid dir', config_dir)
        return

    layer_dir = os.path.join(args.dest_dir, "layers")
    if not os.path.isdir(layer_dir):
        logging.error('%s is not a valid dir', layer_dir)
        return

    layer_db_json_dir = os.path.join(args.dest_dir, 'layer_db_json')
    if not os.path.isdir(layer_db_json_dir):
        # logging.error('%s is not a valid dir', layer_dir)
        logging.debug('make layer_db_json dir ==========> %s' % layer_db_json_dir)
        cmd1 = 'mkdir %s' % layer_db_json_dir
        logging.debug('The shell command: %s', cmd1)
        try:
            subprocess.check_output(cmd1, shell=True)
        except subprocess.CalledProcessError as e:
            print '###################'+e.output+'###################'
            # q_bad_unopen_layers.put('sha256:' + layer_id.split("-")[1]+':cannot-make-dir-error')
            return
    # logging.debug('to ==========> %s', layer_dir)

    dir = {
        'dirname': args.dest_dir,
        # 'manifest_dir': manifest_dir,
        'config_dir': config_dir,
        'layer_dir': layer_dir,
        'layer_db_json_dir': layer_db_json_dir
    }

    dest_dir.append(dir)
    logging.info('dest dir is: %s', dest_dir)

    # logging.info('downloaded_layer_file is: %s', args.downloaded_layer_file)
    # if not os.path.isfile(args.downloaded_layer_file):
    #     logging.error('% is not a valid file', args.downloaded_layer_file)
    #     return

    logging.info('analyzed_layer_file is: %s', args.analyzed_layer_file)
    if not os.path.isfile(args.analyzed_layer_file):
        logging.error('%s is not a valid file', args.analyzed_layer_file)
        return

    logging.info('extracting_dir is: %s', args.extracting_dir)
    if not os.path.isdir(args.extracting_dir):
        logging.error('%s is not a valid file', args.extracting_dir)
        return

    # f_layer_db = open(layer_db_filename, 'w+')args.downloaded_layer_file,
    create_layer_db(args.analyzed_layer_file, args.extracting_dir)

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
    logging.info('create layer json file, consumed time ==> %f', (elapsed / 3600))

    #plt_repeat_layer(images)
    #plt_files_size(images)

if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
