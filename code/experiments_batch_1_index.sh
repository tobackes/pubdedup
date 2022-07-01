codedir=/data_ssd/backests/Repositories/pubdedup/code/;
script=${codedir}pipeline/4_index.sh;

arg=publications;
mode=words;

i=1;
for spechem in restrictions_publications_words restrictions_publications_words_stricter; do
    for scheme in 8281_3021_4441_0_40 skip skip no_generalization; do #8281_3021_4441_0_40 6241_3021_4041_10_40 4261_3021_4041_10_40 no_generalization; do
        i=$((i+1));
        if [[ $scheme == skip ]]; then
            continue;
        fi
        if (( i==10 )); then
            disk=${i};
        else
            disk=0${i};
        fi;
        echo $script $arg $disk $spechem $scheme $mode;
        echo "eval \"\$(conda shell.bash hook)\"; conda activate py38; bash $script $arg $disk $spechem $scheme $mode;" > ${codedir}experiments/${i}_index.sh; chmod a+x ${codedir}experiments/${i}_index.sh;
        wait;
        screen -S INDEX${i} -d -m;
        screen -r INDEX${i} -X stuff "${codedir}experiments/${i}_index.sh"$(echo -ne '\015');
    done;
done
