dir1=/data_ssd/backests/Repositories/pubdedup/;
dir2=/data_ssds/disk01/backests/pubdedup/;

code=${dir1}code/evaluate_minels_v3.py

components=${dir1}components_publications.db
representations=${dir2}representations_test5/representations.db;
features=${dir2}representations_test5/features.db;


python $code $components $representations $features;
