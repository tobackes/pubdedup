dir1=/data_ssd/backests/Repositories/pubdedup/;

arg=$1
spechem=$2
mode=$3 #words bigrams ngrams title
        #fields parts ngrams

dir2=/data_ssds/disk01/backests/pubdedup/;

echo temporary directory: ${SQLITE_TMPDIR};

case $arg in

publications)
    code=${dir1}code/specify_mentions.py
    specification_folder=${dir2}representations_${arg}/${spechem}/
    mentions_before=${dir2}representations_${arg}/mentions_${mode}.db
    mentions=${specification_folder}mentions.db
    restrictions=${dir1}mappings/specification_schemes_${arg}/${spechem}.txt
    mkdir $specification_folder
    cp $mentions_before $mentions;
    nice -n 1 python $code $mentions $restrictions;
    ;;
authors)
    code=${dir1}code/specify_mentions.py
    specification_folder=${dir2}representations_${arg}/${spechem}/
    mentions_before=${dir2}representations_${arg}/mentions_${mode}.db
    mentions=${specification_folder}mentions.db
    restrictions=${dir1}mappings/specification_schemes_${arg}/${spechem}.txt
    mkdir $specification_folder
    cp $mentions_before $mentions;
    nice -n 1 python $code $mentions $restrictions;
    ;;
institutions) #TODO: Test
    code=${dir1}code/specify_mentions.py
    specification_folder=${dir2}representations_institutions_wos/${spechem}/
    mentions_before=${dir2}representations_institutions_wos/mentions_${mode}.db
    mentions=${dir2}representations_institutions_wos/${spechem}/mentions.db
    restrictions=${dir1}mappings/specification_schemes_institutions/${spechem}.txt
    if [ "$spechem" = "restrictions_institutions_fields_threshold" ] || [ "$spechem" = "restrictions_institutions_fields_threshold_mentions" ]; then
        code=${dir1}code/specify_mentions_institutions.py
    fi
    mkdir $specification_folder
    cp $mentions_before $mentions;
    nice -n 1 python $code $mentions $restrictions;
    ;;
institutions_bfd) #TODO: Test
    code=${dir1}code/specify_mentions.py
    specification_folder=${dir2}representations_institutions_bfd/${spechem}/
    mentions_before=${dir2}representations_institutions_bfd/mentions_${mode}.db
    mentions=${dir2}representations_institutions_bfd/${spechem}/mentions.db
    restrictions=${dir1}mappings/specification_schemes_institutions/${spechem}.txt
    if [ "$spechem" = "restrictions_institutions_fields_threshold" ] || [ "$spechem" = "restrictions_institutions_fields_threshold_mentions" ]; then
        code=${dir1}code/specify_mentions_institutions.py
    fi
    mkdir $specification_folder
    cp $mentions_before $mentions;
    nice -n 1 python $code $mentions $restrictions;
    ;;
esac
