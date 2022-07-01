dir1=/data_ssd/backests/Repositories/pubdedup/;
dir2=/data_ssds/disk01/backests/pubdedup/;

code=${dir1}code/disambiguate_v3.py;

query=database;

components=${dir1}components_authors_rid.db;
representations=${dir2}representations_authors/representations_rid.db;
features=${dir2}representations_authors/features_rid.db;

label=$1;

pid=author_test;
configs=${dir1}configs/test_authors.json;

python $code $query ${components}+${representations}+${features}+${label} $pid $configs
