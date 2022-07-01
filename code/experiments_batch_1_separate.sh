codedir=/data_ssd/backests/Repositories/pubdedup/code/;
script=${codedir}pipeline/5_separate.sh;

arg=publications;
method=poset;

i=1;
#TODO: With method==poset, do only run one experiment at a time!
#TODO: In this case I need to set i above to the correct disk number minus one!
for spechem in restrictions_publications_words restrictions_publications_words_stricter; do
    for scheme in 8281_3021_4441_0_40 6241_3021_4041_10_40 4261_3021_4041_10_40 no_generalization; do
        i=$((i+1));
        if (( i==10 )); then
            disk=${i};
        else
            disk=0${i};
        fi;
        echo $script $arg $disk $spechem $scheme $method;
        echo "eval \"\$(conda shell.bash hook)\"; conda activate py38; bash $script $arg $disk $spechem $scheme $method;" > ${codedir}experiments/${i}_separate.sh; chmod a+x ${codedir}experiments/${i}_separate.sh;
        wait;
        screen -S EXPERIMENT${i} -d -m;
        screen -r EXPERIMENT${i} -X stuff "${codedir}experiments/${i}_separate.sh"$(echo -ne '\015');
        #screen -S EXPERIMENT${i} "${codedir}experiments/${i}.sh";
    done;
done
