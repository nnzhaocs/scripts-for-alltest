
from imports import *
# # from images import *
# from layers import *
# from dir import *
# from file import *
# from draw_pic import *
# from utility import *

from run_funcs import *

""" TODO:
    1. put layer json to a single file
     2. fetch all the manifest
     3. get all the tags
"""
""" python main.py -D -L -d dest_dir -a analyzed_layer_file -e extracting_dir -z zip_archival_dir"""
""" python main.py -D -I -d dest_dir -a analyzed_layer_file """


def parseArg():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-D', '--debug',
        help="Print lots of debugging statements",
        action="store_const", dest="loglevel", const=logging.DEBUG,
        default=logging.INFO,
    )

    parser.add_argument(
        '-L', '--createlayerdb',
        help="Create layer json (database)",
        action="store_true",  # dest="loglevel", const=logging.INFO,
    )

    parser.add_argument(
        '-I', '--createimagedb',
        help="Create image json (database)",
        action="store_true",  # dest="loglevel", const=logging.INFO,
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
        '-a', '--analyzed_file',
        help="file which contains analyzed layers/images from downloader module, eg., analyzed_image_file.out",
        dest="analyzed_file",
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

    if args.createlayerdb:
        run_createlayerdb(args)
    if args.createimagedb:
        run_createimagedb(args)


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'
