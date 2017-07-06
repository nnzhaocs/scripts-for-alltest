
import os, argparse, logging
from apscheduler.schedulers.blocking import BlockingScheduler
# from apscheduler.schedulers.blocking import BlockingScheduler

"""python main.py -D -d dest_dir -l analyzed_layer_file -e extracting_dir"""


def scan_and_create_layer_db(debug, dest_dir, analyzed_layer_file, extracting_dir):
    if debug:
        sstr = 'python main.py -D'
    else:
        sstr = 'python main.py'
    cmd = sstr+' -d %s -l %s -e %s' % (dest_dir, analyzed_layer_file, extracting_dir)
    print cmd
    rc = os.system(cmd)
    assert (rc == 0)


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

    parser.add_argument(
        '-l', '--analyzed_layer_file',
        help="file which contains analyzed layers from downloader module, eg., finished_layer_list.out",
        dest="analyzed_layer_file",
    )

    return parser.parse_args()


def main():
    args = parseArg()
    if args.loglevel:
        debug = True
    else:
        debug = False

    scheduler = BlockingScheduler()
    scheduler.add_job(scan_and_create_layer_db, 'interval', hours=10, args=(debug, args.dest_dir, args.analyzed_layer_file, args.extracting_dir))
    scheduler.start()

if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'