
import numpy as np
import os

image_name_file_abs = "image-description.txt"
#output_file_abs = "description_only.txt"
#output_fd = open(output_file_abs, "w")

image_arr = np.empty((0, 2))
with open(image_name_file_abs) as fd:
    for line in fd:
        #print line.split("\t")
        image_description = line.split("\t")[0]
        if image_description == " ":
            continue
        image_name = line.split("\t")[1].replace("\n","")
        image_arr = np.append(image_arr, ([image_name, image_description],), axis=0)
        #output_fd.write(image_description+"\t"+image_name+"\n")

#print image_arr
print "finished loading image description"
#data = numpy.loadtxt('image-description.txt', delimiter = '\t', dtype = str)
data = image_arr
#print data
numrows = len(data)
numcols = len(data[0])

print numrows
print numcols

unique_name = []
#unique_arry = [] #numpy.zeros(shape=(numrows, numcols))
fout = open('unique_name_description.txt', 'w+')
for i in range(numrows):
	if data[i][0] not in unique_name:
		#unique_arry.append(data[i])
		for j in range(numcols):
			fout.writelines(data[i,j])
			fout.writelines('\t')
		fout.writelines('\n')
		unique_name.append(data[i][0])

#out = np.array(unique_arry)
fout.close()

print "saving image descriptions"
#numpy.savetxt('unique_name-description.out', out, fmt = "%s\t%s")
