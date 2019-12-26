
import random
import sys

inputfile = sys.argv[1]
samplesize = int(sys.argv[2])
outputfile = sys.argv[3]

print(inputfile, samplesize, outputfile)

layers = []

with open(sys.argv[1]) as f:
	layers = [line.rstrip('\n') for line in f]	

print len(layers)
newsample = random.sample(layers, samplesize)

with open(sys.argv[3], 'w') as fp:
    fp.writelines("%s\n" % layer for layer in newsample)

