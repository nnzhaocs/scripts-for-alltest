


# absolute path!!!

cd sampledlayers

ls > ../sampledlayers.names.lst

cd ../uncompressedlayers

cat ../sampledlayers.names.lst | parallel --jobs 30 mkdir {}

#

cd sampledlayers

cat ../sampledlayers.names.lst | parallel --jobs 30 tar -zxpf {} -C ../uncompressedlayers/{} 

