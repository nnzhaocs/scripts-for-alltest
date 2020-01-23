
namedigest () {
     sha256=$(sha256sum $1 | awk -F' ' '{print $1}')
     mkdir -p "uniqfiles/${sha256:0:2}"
     mv $1 "uniqfiles/${sha256:0:2}/$sha256"

}

export -f namedigest

cat totalfilesdir.lst | parallel -j 10 namedigest








