
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
        '-c', '--create',
        help="Create database",
        action="store_true"
    )
    return parser.parse_args()

def main():
    args = parseArg()

    logging.basicConfig(level=args.loglevel)

    if args.create:
        images = load_images()
        cal_layer_repeats(images)
        with open(db_filename, 'w+') as f_out:
            json.dump(images, f_out)

        print "here loaded images to file: database_json!"
    else:
        logging.info('loading images ...')
        with open(db_filename, 'r') as f_out:
            images = json.load(f_out)

        logging.info('analyzing/plotting images/layers ...')

        #plt_repeat_layer(images)
        #plt_files_size(images)

if __name__ == '__main__':
	print "start!"
	main()
	print "finished!"
