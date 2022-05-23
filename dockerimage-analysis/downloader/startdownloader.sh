
td="/root/Downloads"
lf="/root/Downloads/downloadedlayers.lst"
if="/root/Downloads/downloadedimages.lst"
hotlys="/root/scripts-for-alltest/dockerimage-analysis/downloader/top100.lst"


[ ! -d "$td" ] && mkdir "$td"

[ ! -s "$lf" ] && touch "$lf"

[ -s "$lf" ] && : > "$lf"

[ ! -s "$if" ] && touch "$if"

[ -s "$if" ] && : > "$if"

cp "$hotlys" "$td""/image_name.lst"

docker pull nnzhaocs/downloader:stable

docker run -v /root/Downloads/:/root/Downloads nnzhaocs/downloader:stable

