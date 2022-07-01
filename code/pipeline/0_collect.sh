dir1=/data_ssd/backests/Repositories/pubdedup/;
dir2=/data_ssds/disk01/backests/pubdedup/;

arg=$1

echo temporary directory: ${SQLITE_TMPDIR};

case $arg in

publications)
    code=${dir1}code/load_mentions_publications.py
    infolder=${dir1}data/core_2020/ #data/core_2018-03-01_fulltext/
    outfolder=${dir2}representations_publications/mentions/
    outfile=${dir2}representations_publications/mentions_ngrams.db
    modifications=${dir1}resources/transitions.txt
    termfreqs=${dir1}resources/core_term_freqs.db
    namefreqs=${dir1}resources/name_freqs.db
    numjobs=64 # WARNING: If you reduce this number you need to remove first the files in mentions/ because otherwise the old remaining ones will be added in the second step
    nice -n 1 python $code $infolder $outfolder $modifications $termfreqs $namefreqs $numjobs;
    wait
    bash ${dir1}code/combine_representations.sh $outfolder $outfile
    ;;
authors)
    code=${dir1}code/load_mentions_authors.py
    infile=${dir1}representations_authors/all_names_IDadjust_newer.db
    mentions=${dir2}representations_authors/mentions_ngrams.db #TODO: Specify _fields.db _parts.db _ngrams.db
    nice -n 1 python $code $infile $mentions;
    ;;
institutions)
    code=${dir1}code/load_mentions_institutions_v2.py
    infile=/data_ssd/backests/Repositories/instipro_v2/representations/6/institutions/wos.db
    mentions=${dir2}representations_institutions_wos/mentions_ngrams.db #TODO: Specify _fields.db _ngrams.db
    nice -n 1 python $code $infile $mentions;
    ;;
institutions_bfd)
    code=${dir1}code/load_mentions_institutions_v2.py
    infile=/data_ssd/backests/Repositories/instipro_v2/representations/6/institutions/bielefeld.db
    mentions=${dir2}representations_institutions_bfd/mentions_ngrams.db #TODO: Specify _fields.db _ngrams.db
    nice -n 1 python $code $infile $mentions;
    ;;
esac
