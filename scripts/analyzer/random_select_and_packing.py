
import random
import sys

inputfile = sys.argv[1]
samplesize = sys.argv[2]
outputfile = sys.argv[3]

layers = []

with open(sys.argv[1]) as f:
	layers = [line.rstrip('\n') for line in f]	

newsample = random.sample(layers, samplesize)

with open(sys.argv[3], 'w') as fp:
    fp.writelines("%s\n" % layer for layer in newsample)

