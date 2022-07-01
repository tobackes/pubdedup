dir1=/data_ssd/backests/Repositories/pubdedup/;
dir2=/data_ssds/disk01/backests/pubdedup/;

code=${dir1}code/disambiguate_v3.py;

query=database;

components=${dir1}institutions_wos_components.db;
representations=${dir1}representations_institutions_v2/representations_wos.db;
features=${dir1}representations_institutions_v2/features_wos.db;

label=$1;

pid=institutions_test;

configs=${dir1}configs/test_institutions.json;


python $code $query ${components}+${representations}+${features}+${label} $pid $configs
