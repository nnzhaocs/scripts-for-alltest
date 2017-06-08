
from imports import *
from images import *
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
    parser.add_argument(
        '-y', '--yes',
        help="Run",
        action="store_true"
    )
    return parser.parse_args()

def main():
    args = parseArg()

    logging.basicConfig(level=args.loglevel)

    images = load_images()
    print "here loaded images!"

    logging.info('analyzing layers ...')
    cal_layer_repeats(images)
    plt_repeat_layer(images)
    plt_files_size(images)

if __name__ == '__main__':
	print "start!"
	main()
	print "finished!"
