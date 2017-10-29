

awk 'NF>=2' image-description.txt > output_file.txt
uniq -f 1 output_file.txt > output_file.uniq
perl -nle 'print if m{^[[:ascii:]]+$}' output_file.uniq > all_english_unique_image_description.uniq
awk -F'\t' '{print $1}' all_english_unique_image_description.uniq-cp > col1.uniq
awk -F'\t' '{print $2}' all_english_unique_image_description.uniq-cp > col2.uniq
paste -d '\t' col1.uniq col2.uniq > 2col.uniqs
wc -L col2.uniq
image_arr=np.loadtxt('2col.uniqs',delimiter='\t', dtype='S512')
