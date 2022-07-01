codedir=/data_ssd/backests/Repositories/pubdedup/code/;
script=${codedir}pipeline/3_generalize.sh;

arg=authors;
mode=fields;

i=1;
for spechem in restrictions_authors_fields_firstinit restrictions_authors_fields_firstinit_mentions; do
    for scheme in one_up surname_allinits surname_firstinit surname_no3rd surname_no3rd_no2ndname no_generalization; do
        i=$((i+1));
        #if (( i==10 )); then
        #    disk=${i};
        #else
        #    disk=0${i};
        #fi;
        disk=08;
        echo $script $arg $disk $spechem $scheme $mode;
        echo "eval \"\$(conda shell.bash hook)\"; conda activate py38; bash $script $arg $disk $spechem $scheme $mode;" > ${codedir}experiments/authors_${i}.sh; chmod a+x ${codedir}experiments/authors_${i}.sh;
        wait;
        #screen -S AUTHORS${i} -d -m;
        screen -r AUTHORS${i} -X stuff "${codedir}experiments/authors_${i}.sh"$(echo -ne '\015');
        #screen -S EXPERIMENT${i} "${codedir}experiments/${i}.sh";
    done;
done
