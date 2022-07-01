dir1=/data_ssd/backests/Repositories/pubdedup/;

arg=$1
disk=$2
spechem=$3
scheme=$4
method=$5 #simhash title poset
size=$6
offset=$7

dir2=/data_ssds/disk${disk}/backests/pubdedup/;

code=${dir1}code/disambiguate_v3.py;

query=database;

arg=$1

echo temporary directory: ${SQLITE_TMPDIR};

case $arg in

publications)
    components=${dir2}representations_publications/${spechem}/${scheme}/components_${method}.db;
    representations=${dir2}representations_publications/${spechem}/${scheme}/representations.db;
    features=${dir2}representations_publications/${spechem}/${scheme}/features.db;
    pid=publications_test;
    configs=${dir1}configs/$spechem.json;
    ;;
authors)
    components=${dir2}representations_authors/${spechem}/${scheme}/components.db;
    representations=${dir2}representations_authors/${spechem}/${scheme}/representations.db;
    features=${dir2}representations_authors/${spechem}/${scheme}/features.db;
    pid=authors_test;
    configs=${dir1}configs/test_authors.json;
    ;;
institutions)
    components=${dir2}representations_institutions_wos/${spechem}/${scheme}/components.db;
    representations=${dir2}representations_institutions_wos/${spechem}/${scheme}/representations.db;
    features=${dir2}representations_institutions_wos/${spechem}/${scheme}/features.db;
    pid=institutions_test;
    configs=${dir1}configs/test_institutions_wos.json;
    ;;
institutions_bfd)
    components=${dir2}representations_institutions_bfd/${spechem}/${scheme}/components.db;
    representations=${dir2}representations_institutions_bfd/${spechem}/${scheme}/representations.db;
    features=${dir2}representations_institutions_bfd/${spechem}/${scheme}/features.db;
    pid=institutions_test;
    configs=${dir1}configs/test_institutions_bfd.json;
    ;;
esac

#label=`sqlite3 $components "SELECT label FROM (SELECT label,COUNT(*) as freq FROM components GROUP BY label) WHERE freq=${size} LIMIT ${offset},1"`
label=$offset

python $code $query ${components}'+'${representations}'+'${features}'+'${label} $pid $configs
