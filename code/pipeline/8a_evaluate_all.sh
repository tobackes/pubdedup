arg=$1 #institutions_bfd institutions_wos
disk=$2
spechem=$3
scheme=$4
mode=$5 #words bigrams ngrams title
        #fields parts ngrams
method=$6 #poset simhash single

script=/data_ssd/backests/Repositories/pubdedup/code/pipeline/8_evaluate_components_v2.sh

for min_gold_size in 1 2; do
    for max_gold_size in 10 25 50 100 1000 10000; do
        for max_label_size in 10 100 1000 10000 100000 1000000; do
            bash $script $arg $disk $spechem $scheme $min_gold_size $max_gold_size $max_label_size pair $mode $method;
        done;
    done;
done

if [ "$arg" == "publications" ]; then
    bash $script $arg $disk $spechem $scheme 1 1000 100000 core $mode $method;
fi
