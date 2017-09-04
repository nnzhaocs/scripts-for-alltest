
# import sys
# sys.path.append('../libraries/')
# from config import *
# from draw_pic import *
from get_metrics_data import *
# from plot_graph import *


def parseArg():
    parser.add_argument(
        '-P', '--plotgraph',
        help="plot graphs",
        action="store_true",  # dest="loglevel", const=logging.INFO,
    )

    parser.add_argument(
        '-G', '--getmetricsdata',
        help="get metrics data",
        action="store_true",  # dest="loglevel", const=logging.INFO,
    )

    parser.add_argument(
        '-G', '--generatejoblist',
        help="generate analyzer job list",
        action="store_true",  # dest="loglevel", const=logging.INFO,
    )


def main():
    args = parseArg()
    print args
    #logging.basicConfig(level=args.loglevel)
    fmt="%(funcName)s():%(lineno)i: %(message)s %(levelname)s"
    logging.basicConfig(level=args.loglevel, format=fmt)
    load_config()

    if args.getmetricsdata:
        run_getmetricsdata()

    if args.plotgraph:
        run_plotgraph()

    if args.generatejoblist:
        run_generatejoblist()
    #
    # if args.listdirfiles:
    #     run_listdirfiles()


if __name__ == '__main__':
    print 'start!'
    main()
    print 'finished!'

