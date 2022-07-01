# This needs to be executed from a machine that has graphviz installed

schemefile=$1;

script=/home/tobias/GPU-SERVER/data_ssd/backests/Repositories/pubdedup/code/visualize_generalization_scheme.py
folder=/home/tobias/GPU-SERVER/data_ssd/backests/Repositories/pubdedup/documentation/;

filename="${schemefile##*/}";
scheme="${filename%.*}";
echo $scheme;


python $script $schemefile ${folder}${scheme}.dot

tred ${folder}${scheme}_gens.dot > ${folder}${scheme}_gens_reduced.dot

sed '$d' ${folder}${scheme}_gens_reduced.dot | cat - ${folder}${scheme}_rule.txt  > ${folder}${scheme}_final.dot
