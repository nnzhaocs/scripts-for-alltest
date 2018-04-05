
from config import *

def test_decompression():
    layer_dict = load_layer_dict()
    load_layer_dict(layer_dict)




def load_testing_layer_lst(layer_dict):
    layer_fname = []
    with open(testing_layer_lst_absfilename) as f:
        for line in f:
            line = line.rstrip('\n')
            layer_filename = layer_dict[line]
            layer_fname.append(layer_filename)


def load_layer_dict():
    with open(layer_dict_fname) as f:
        layer_dict = json.load(f)

    return layer_dict


def process_layer(layer_filename):
