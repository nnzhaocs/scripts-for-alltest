
hulk2filestorelst="/home/nannan/sampled.lst"
hulk2filestoredir="/home/nannan/sampledfilesdir"

#rados -p dedup_base put {} hulk2filestoredir/{}

# store files into deduped chunks
while read line ; do
	rados -p dedup_base put $line "$hulk2filestoredir/$line" 
	touch "$hulk2filestoredir/$line-chunk"
	rados -p dedup_chunk put $line-chunk "$hulk2filestoredir/$line-chunk"
	#cat $hulk2filestorelst | rados -p dedup_chunk put {}-chunk hulk2filestoredir/{}-chunk
	offset=$(stat --printf="%s" $hulk2filestoredir/$line)
	rados -p dedup_base set-chunk $line 0 $offset --target-pool dedup_chunk 0 --with-reference  
	rados -p dedup_base set-redirect $line --target-pool dedup_chunk "$line-chunk"
done

rados ls -p dedup_base
rados ls -p dedup_chunk


