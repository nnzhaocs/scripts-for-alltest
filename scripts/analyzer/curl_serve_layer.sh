
#!/bin/bash

SENT="false"
RECIEVE="false"
FILE_OR_DATA="DATA"
FILE=""
DATA=""
TARGET=""

function print_usage(){
    echo "Usage: file.sh [-s|-r] [-f -F FILE | -d data ] -t url_target"
}

function sent_file(){
    FILE=$1
    TARGET=$2
    echo "send file: filename=$FILE,path=$TARGET"
    curl -X POST -F "file=@""$FILE" "$TARGET"
}

function sent_data(){
    DATA=$1
    TARGET=$2
    echo "send data: data=$DATA,path=$TARGET"
    curl -X POST -d "$DATA" "$TARGET"
}

function recieve_file(){
    FILE=$1
    SOURCE=$2
    echo "recv file: file=$FILE,path=$TARGET"
    curl "$TARGET" -o "$FILE"
}

function recieve_data(){
    #DATA=$1
    TARGET=$1
    #echo "rect data: data=$DATA,path=$TARGET"
    echo "path:$TARGET"
    DATA=`curl "$TARGET"` 
    echo "rect data: data=$DATA,path=$TARGET"
}


function print_args(){
    echo "ARGS ARE: ""SENT = $SENT RECIEVE = $RECIEVE FILE_OR_DATA = $FILE_OR_DATA FILE = $FILE DATA = $DATA TARGET = $TARGET"
}
while getopts 'srfF:d:t:' OPT; do
    case $OPT in
        s)
            SENT="true";;
        r)
            RECIEVE="true";;
        f)
            FILE_OR_DATA="FILE";;
        F)
            FILE="$OPTARG";;
        d)
            DATA="$OPTARG";;
        t)
            TARGET="$OPTARG";;

        ?)
            print_usage;exit -1;;
    esac
done

print_args

if [ "FILE" = "$FILE_OR_DATA" ]
then

if [ "true" = "$SENT" ];then
    if [ X"" = X"$FILE" ];then
        echo "-f should not be empty when send file."
        exit -1
    fi
    if [ x"" = x"$TARGET" ];then
        echo "-t should not be empty when send data."
        exit -1
    fi

    sent_file $FILE $TARGET
    exit 0
fi

if [ "true" = "$RECIEVE" ];then
    if [ X"" = X"$FILE" ];then
        echo "-f should not be empty when recv file."
        exit -1
    fi
    if [ x"" = x"$TARGET" ];then
        echo "-t should not be empty when recv file."
        exit -1
    fi
    recieve_file $FILE $TARGET
    exit 0
fi

else

if [ "true" = "$SENT" ];then
    if [ X"" = X"$DATA" ];then
        echo "-d should not be empty when send data."
        exit -1
    fi
    if [ x"" = x"$TARGET" ];then
        echo "-t should not be empty when send data."
        exit -1
    fi

    sent_data $DATA $TARGET
    exit 0
fi
if [ "true" = "$RECIEVE" ];then
    #if [ X"" = X"$DATA" ];then
    #    echo "-d should not be empty when recv data."
    #    exit -1
    #fi
    if [ x"" = x"$TARGET" ];then
        echo "-t should not be empty when recv data."
        exit -1
    fi
    recieve_data $TARGET
    exit 0
fi

fi

print_usage

