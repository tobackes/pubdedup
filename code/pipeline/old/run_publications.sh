dir1=/data_ssd/backests/Repositories/pubdedup/;
dir2=/data_ssds/disk01/backests/pubdedup/;

code=${dir1}code/disambiguate_v3.py;

query=database;

components=${dir1}components_publications.db;
representations=${dir2}representations_test5/representations.db;
features=${dir2}representations_test5/features.db;

label=$1;

pid=institutions_test;

configs=${dir1}configs/test.json;


python $code $query ${components}+${representations}+${features}+${label} $pid $configs
