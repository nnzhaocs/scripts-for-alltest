
#! /bin/bash

# first before doing this test, we disabled disk cache and fs page cache
# sudo hdparm -W0 /dev/sda; --direct=1

filecntlst=(100 1000 100000)
bslst=(1 4 16 64 256 1024)
typelst=("read" "write" "randread" "randwrite")
jobslst=(2 4 8 16 32)
totalruntime=600
totaliosize='2g'
# blocksize=4kb
outputfilename='layeriotest.lst'

test_filesize () {

    for testiosize in ${bslst[@]}; do
        echo "testing: type: $1, fcnt: $2, numjobs: $3"
       
        testsize=$(echo "$testiosize*$2"|bc)
        echo "testsize:  $testsize"
        #docker run --rm ljishen/fio --name=read --directory=/ --nrfiles=1000 --size=1000k --io_size=1k --direct=1 --rw=read --numjobs=1 --runtime=300 --group_reporting
        docker run --rm ljishen/fio --name=filesize --directory=/ --nrfiles=$2 --size=$testsize"k" --io_size=$totaliosize --direct=1 --rw=$1 --numjobs=$3 --runtime=$totalruntime --group_reporting &> tmp
        echo "docker run --rm ljishen/fio --name=filesize --directory=/ --nrfiles=$2 --size=$testsize"k" --io_size=$testiosize"k" --direct=1 --rw=$1 --numjobs=$3 --runtime=$totalruntime --group_reporting" >> $outputfilename

        grep "IOPS" tmp >> $outputfilename
    done
}

test_layer_filesize () {
    fcnt=1000
    numjobs=1
    for tp in ${typelst[@]}; do
        echo $tp
        test_filesize $tp $fcnt $numjobs
        sleep 1
    done
}

test_layer_filesize

test_layer_filesizejobs () {
    fcnt=100
    #numjobs=1
    for testjobs in ${jobslst[@]}; do
        for tp in ${typelst[@]}; do
            echo $tp
            test_filesize $tp $fcnt $testjobs
            sleep 1
        done
    done
}

#test_layer_filesizejobs
# ============================ directories =========================
dirname='/$jobnum'

test_directories () {

     for testjobs in ${jobslst[@]}; do
         echo "testing: type: $1, fcnt: $2, iosize: $3, testdepth: $4"

         testsize=$(echo "$2*$3"|bc)
         echo "testsize:  $testsize"
         format='/$jobnum'
         i=1
         while [ $i -lt $4 ] ; do
            echo "$i"
            format=${format}'/$jobnum'
            ((i++))
         done
         echo "$format"
         format=${format}'/$filenum.f'
         #docker run -ti --rm nnzhaocs/fio fio --name=read --nrfiles=10 --size=10k --filename_format='/$jobnum/$filenum.f' --io_size=1k --direct=1 --rw=read --numjobs=2  --runtime=300 --group_reporting
         sudo docker run --rm nnzhaocs/fio fio --name=directory --nrfiles=$2 --size=$testsize"k" --filename_format=$format --io_size=$3"k" --direct=1 --rw=$1 --numjobs=$testjobs --runtime=$totalruntime --group_reporting &> tmp
         echo "docker run --rm nnzhaocs/fio fio --name=directory --nrfiles=$2 --size=$testsize"k" --filename_format=$format --io_size=$3"k" --direct=1 --rw=$1 --numjobs=$testjobs --runtime=$totalruntime --group_reporting" >> $outputfilename

         grep "IOPS" tmp >> $outputfilename
     done

}

dirdepthlst=(2 4 8 16)

test_layer_directories () {
     fcnt=100
     #numjobs=1
     bs=16
     for testdepth in ${dirdepthlst[@]}; do
         for tp in ${typelst[@]}; do
             echo $tp
             test_directories $tp $fcnt $bs $testdepth
             sleep 1
         done
     done
}


#test_layer_directories


#========================== test layers =================

# fio --name=directory --nrfiles=100 --size=1600k --filename_format='/$jobnum/$jobnum/$filenum.f' --io_size=16k --direct=1 --rw=write --create_only=1 --numjobs=32 --runtime=300 --group_reporting
# CMD  for i in {0..2000..1}; do \
#        mkdir -p "/$i/$i/$i/$i/$i/$i/$i/$i/$i/$i/$i/$i/$i/$i/$i/$i/" ;  \
#     done
# Layer size: 50M, 32 dir, depth 2, 100 files, 16 kb each.
# nnzhaocs/fiodifflayertest

layercntlst=(6 12 18 24 30)

test_layers () {

      #for testjobs in ${jobslst[@]}; do
          echo "testing: type: $1, fcnt: $2, iosize: $3, testdepth: $4, testlayercnt: $5, jobs: $6"

          testsize=$(echo "$2*$3"|bc)
          echo "testsize:  $testsize"
          format='/$jobnum/$jobnum/$filenum.f'
      #    i=1
      #    while [ $i -lt $4  ] ; do
      #       echo "$i"
      #       format=${format}'/$jobnum'
      #       ((i++))
      #    done
      #    echo "$format"
      #    format=${format}'/$filenum.f'
          #docker run -ti --rm nnzhaocs/fio fio --name=read --nrfiles=10 --size=10k --filename_format='/$jobnum/$filenum.f' --io_size=1k --direct=1 --rw=read --numjobs=2  --runtime=300 --group_reporting
          sudo docker run --rm nnzhaocs/fiodifflayertest-$5 fio --name=directory --nrfiles=$2 --size=$testsize"k" --filename_format=$format --io_size=$3"k" --direct=1 --rw=$1 --numjobs=$6 --runtime=$totalruntime --group_reporting &> tmp
          echo "docker run --rm nnzhaocs/fiodifflayertest-$5 fio --name=directory --nrfiles=$2 --size=$testsize"k" --filename_format=$format --io_size=$3"k" --direct=1 --rw=$1 --numjobs=$6 --runtime=$totalruntime --group_reporting" >> $outputfilename

          grep "IOPS" tmp >> $outputfilename
      #done
}


test_diff_layers () {
      fcnt=100
      #numjobs=1
      bs=4
      testdepth=2
      jobs=32
      for layercnt in ${layercntlst[@]}; do
          for tp in ${typelst[@]}; do
              echo $tp
              test_layers $tp $fcnt $bs $testdepth $layercnt $jobs
              sleep 1
          done
      done

}

#test_diff_layers
