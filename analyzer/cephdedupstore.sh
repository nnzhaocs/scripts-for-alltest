
hulk2filestorelst="/home/nannan/sampleduniqfilesnames.lst"




#rados -p dedup_base put {} hulk2filestoredir/{}

# store files into deduped chunks
#while read line ; do

storetoceph () {

	hulk2filestoredir="/home/nannan/sampled"

	line=$1
	#echo $line
	rados -p dedup_base put $line "$hulk2filestoredir/$line" 
	
	rados -p dedup_chunk put "$line-chunk"  "$hulk2filestoredir/$line"  #"${hulk2filestoredir_chunk}/$line-chunk"
	
	offset=$(stat --printf="%s" $hulk2filestoredir/$line)
	echo $offset
	rados -p dedup_base set-chunk $line 0 $offset --target-pool dedup_chunk "$line-chunk" 0 --with-reference  
	rados -p dedup_base set-redirect $line --target-pool dedup_chunk "$line-chunk"
}

export -f storetoceph

cat $hulk2filestorelst|parallel -j 6 storetoceph {}

#done<$hulk2filestorelst

rados ls -p dedup_base
rados ls -p dedup_chunk

# # awk -F'/' '{print $7}' sampleduniqfiles.lst >sampleduniqfilesnames.lst
