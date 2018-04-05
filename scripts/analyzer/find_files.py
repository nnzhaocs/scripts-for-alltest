
from config import *
#from dir.py import *
from load_files import *
"""TODO
Multi-processing

"""


def find_files():
    if not construct_layer_info_or_not:
        layer_dict = construct_layer_info()
    else:
        with open(layer_dict_fname) as f:
            layer_dict = json.load(f)

    layer_file_dict = find_layer_filename()
    cnt = 0
    for layer_id, file_lst in layer_file_dict.items():
	cnt = cnt + 1
	try: 
            layer_fname = layer_dict[layer_id]
        except:
	    print("Cannot find layer_id in this machine's layer directories ################ ", layer_id)
	    continue

        #"""decompress the layer to extracting dir"""
        load_dirs(layer_fname, file_lst, output_dir, cnt)
        #"""get the files"""


def construct_layer_info():
    layer_dict = {}
    for dir in layer_dirs:
        for path, subdirs, files in os.walk(dir):
            for f in files:
                layer_filename = os.path.join(os.path.join(path, f))
                layer_id = 'sha256:' + layer_filename.split("-")[1]
                layer_dict[layer_id] = layer_filename

    with open(layer_dict_fname, 'w+') as f_out:
        json.dump(layer_dict, f_out)

    #return layer_dict


def find_layer_filename():
    layer_file_ldict = defaultdict(list)
    with open(find_file_lst_absfilename) as f:
        for line in f:
            line = line.rstrip('\n')
            layer_id = line.split(',', 1)[0] # split by \t ???????
            #layer_filename = layer_dict[layer_id]
            filename = line.split(',', 1)[1] # split by \t ???????

            layer_file_ldict[layer_id].append(filename)

    return layer_file_ldict
