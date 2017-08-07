
import sys
sys.path.append('../libraries/')
from config import *
from draw_pic import *
from get_metrics_data import *
from plot_graph import *

parser.add_argument(
    '-P', '--plotgraph',
    help="plot graphs",
    action="store_true",  # dest="loglevel", const=logging.INFO,
)

parser.add_argument(
    '-R', '--analyzelayer',
    help="analyze the layer",
    action="store_true",  # dest="loglevel", const=logging.INFO,
)

def run_analyzelayer(args):
    start = time.time()
    logging.info('Input dir name: %s', args.dest_dir)
    if not os.path.isdir(args.dest_dir):
        logging.error('%s is not a valid dir', args.dest_dir)
        return

    # config_dir = os.path.join(args.dest_dir, "configs")
    # if not os.path.isdir(config_dir):
    #     logging.error('%s is not a valid dir', config_dir)
    #     return

    layer_dir = os.path.join(args.dest_dir, "layers")
    if not os.path.isdir(layer_dir):
        logging.error('%s is not a valid dir', layer_dir)
        return

    layer_db_json_dir = os.path.join(args.dest_dir, layer_db_json_name)
    if not os.path.isdir(layer_db_json_dir):
        logging.error('%s is not a valid dir', layer_db_json_dir)
        return

    # logging.info('extracting_dir is: %s', args.extracting_dir)
    # if not os.path.isdir(args.extracting_dir):
    #     logging.error('%s is not a valid file', args.extracting_dir)
    #     return

    dir = {
        'dirname': args.dest_dir,
        # 'config_dir': config_dir,
        'layer_dir': layer_dir,
        'layer_db_json_dir': layer_db_json_dir,
        # 'extracting_dir': args.extracting_dir
    }

    dest_dir.append(dir)
    logging.info('dest dir is: %s', dest_dir)

    # logging.info('analyzed_layer_file is: %s', args.analyzed_file)
    # if not os.path.isfile(args.analyzed_file):
    #     logging.error('%s is not a valid file', args.analyzed_file)
    #     return

    # logging.info('layer_list_file is: %s', args.layer_list_file)
    # if not os.path.isfile(args.layer_list_file):
    #     logging.error('%s is not a valid file', args.layer_list_file)
    #     return

    # create_layer_db(args.analyzed_file, args.layer_list_file)
    layer_distribution(args)

    elapsed = time.time() - start
    logging.info('analyze layer json files, consumed time ==> %f', (elapsed / 3600))


def run_plot(args):
    plot_all(args)

    if args.analyzelayer:
        run_analyzelayer(args)

    if args.plotgraph:
        print "start"
        run_plot(args)