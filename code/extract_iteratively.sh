toplevel=$1;
aggfolder1=$2;
aggfolder2=$3;

aggfolders=($aggfolder1 $aggfolder2);

counter=0;
for filepath in ${toplevel}*.tar.xz; do
    aggfolder=${aggfolders[$(($counter % 2))]};
    filename=${filepath##*/};
    foldername=${filename%%.*};
    folderpath=${aggfolder}${foldername}/;
    echo $filepath $foldername $folderpath;
    mkdir $folderpath;
    tar xf $filepath --directory $folderpath;
    return_code=$?;
    if [ $return_code == 0 ]; then
        echo success extracting;
        echo not currently removing $filepath; #rm $filepath;
        find ${aggfolder}${foldername}/ -name '*.json' -exec cat {} >> ${aggfolder}${foldername}.json \; -exec echo >> ${aggfolder}${foldername}.json \;;
        return_code=$?;
        if [ $return_code == 0 ]; then
            echo success aggregating;
            echo removing everything in $folderpath; rm -r ${folderpath};
        fi
    fi
    counter=$((counter+1));
done
