
from imports import *
from images import *
from layers import *
from dir import *
#from file import *
from draw_pic import *
from utility import *
from jobdivider import *
from analyzer_layers import *


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

    layer_db_json_dir = os.path.join(args.dest_dir, 'layer_db_json_bison03p')
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


def run_createimagedb(args):
    start = time.time()
    logging.info('please put all the manifest files into the dest_dir/manifest directory!')

    logging.info('Input dir name: %s', args.dest_dir)
    if not os.path.isdir(args.dest_dir):
        logging.error('%s is not a valid dir', args.dest_dir)
        return

    manifest_dir = os.path.join(args.dest_dir, "manifests")
    if not os.path.isdir(manifest_dir):
        logging.error('%s is not a valid dir', manifest_dir)
        return

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
        logging.error('%s is not a valid dir', layer_db_json_dir)
        return

    image_db_json_dir = os.path.join(args.dest_dir, 'image_db_json')
    if not os.path.isdir(image_db_json_dir):
        logging.debug('make image_db_json dir ==========> %s' % image_db_json_dir)
        cmd1 = 'mkdir %s' % image_db_json_dir
        logging.debug('The shell command: %s', cmd1)
        try:
            subprocess.check_output(cmd1, shell=True)
        except subprocess.CalledProcessError as e:
            print '###################' + e.output + '###################'
            return

    dir = {
        'dirname': args.dest_dir,
        'manifest_dir': manifest_dir,
        'config_dir': config_dir,
        'layer_dir': layer_dir,
        'layer_db_json_dir': layer_db_json_dir,
        'image_db_json_dir': image_db_json_dir
    }

    dest_dir.append(dir)
    logging.info('dest dir is: %s', dest_dir)

    logging.info('analyzed_image_file is: %s', args.analyzed_file)
    if not os.path.isfile(args.analyzed_file):
        logging.error('%s is not a valid file', args.analyzed_file)
        return

    create_image_db(args.analyzed_image_file)

    elapsed = time.time() - start
    logging.info('create image json file, consumed time ==> %f', (elapsed / 3600))


def run_createlayerdb(args):
    start = time.time()
    logging.info('Input dir name: %s', args.dest_dir)
    if not os.path.isdir(args.dest_dir):
        logging.error('%s is not a valid dir', args.dest_dir)
        return

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
        logging.debug('make layer_db_json dir ==========> %s' % layer_db_json_dir)
        cmd1 = 'mkdir %s' % layer_db_json_dir
        logging.debug('The shell command: %s', cmd1)
        try:
            subprocess.check_output(cmd1, shell=True)
        except subprocess.CalledProcessError as e:
            print '###################' + e.output + '###################'
            return

    logging.info('extracting_dir is: %s', args.extracting_dir)
    if not os.path.isdir(args.extracting_dir):
        logging.error('%s is not a valid file', args.extracting_dir)
        return

    dir = {
        'dirname': args.dest_dir,
        'config_dir': config_dir,
        'layer_dir': layer_dir,
        'layer_db_json_dir': layer_db_json_dir,
        'extracting_dir': args.extracting_dir
    }

    dest_dir.append(dir)
    logging.info('dest dir is: %s', dest_dir)

    logging.info('analyzed_layer_file is: %s', args.analyzed_file)
    if not os.path.isfile(args.analyzed_file):
        logging.error('%s is not a valid file', args.analyzed_file)
        return

    logging.info('layer_list_file is: %s', args.layer_list_file)
    if not os.path.isfile(args.layer_list_file):
        logging.error('%s is not a valid file', args.layer_list_file)
        return

    create_layer_db(args.analyzed_file, args.layer_list_file)

    elapsed = time.time() - start
    logging.info('create layer json file, consumed time ==> %f', (elapsed / 3600))


def run_jobdivider(args):
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

    job_list_dir = os.path.join(args.dest_dir, 'job_list_dir')
    if not os.path.isdir(job_list_dir):
        logging.debug('make layer_db_json dir ==========> %s' % job_list_dir)
        cmd1 = 'mkdir %s' % job_list_dir
        logging.debug('The shell command: %s', cmd1)
        try:
            subprocess.check_output(cmd1, shell=True)
        except subprocess.CalledProcessError as e:
            print '###################' + e.output + '###################'
            return

    dir = {
        'dirname': args.dest_dir,
        # 'config_dir': config_dir,
        'layer_dir': layer_dir,
        'job_list_dir': job_list_dir
    }

    dest_dir.append(dir)
    logging.info('dest dir is: %s', dest_dir)

    # logging.info('analyzed_layer_file is: %s', args.analyzed_file)
    # if not os.path.isfile(args.analyzed_file):
    #     logging.error('%s is not a valid file', args.analyzed_file)
    #     return
    #
    # logging.info('extracting_dir is: %s', args.extracting_dir)
    # if not os.path.isdir(args.extracting_dir):
    #     logging.error('%s is not a valid file', args.extracting_dir)
    #     return

    create_job_list()

    elapsed = time.time() - start
    logging.info('create job list file, consumed time ==> %f', (elapsed / 3600))
