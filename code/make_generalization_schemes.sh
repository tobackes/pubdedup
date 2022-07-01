code=/data_ssd/backests/Repositories/pubdedup/code/ILP_v2.py;

schemes=/data_ssd/backests/Repositories/pubdedup/mappings/generalization_schemes/;

for weights in "8 2 8 1" "4 2 4 1" "6 2 4 1" "4 2 6 1"; do

    for bases in "4 4 4 1" "4 0 5 1" "4 0 4 1"; do

        for slack in 0 10; do

            for maxgens in 20 40 80; do

                name_weight="$(echo -e "${weights}" | tr -d '[:space:]')";
                name_base="$(echo -e "${bases}" | tr -d '[:space:]')";

                echo $name_weight $name_base $slack $maxgens;

                python $code $weights $bases $slack $maxgens ${schemes}${name_weight}_3021_${name_base}_${slack}_${maxgens}.txt;

            done;
        done;
    done;
done
