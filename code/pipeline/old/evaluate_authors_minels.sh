dir1=/data_ssd/backests/Repositories/pubdedup/;
dir2=/data_ssds/disk01/backests/pubdedup/;

code=${dir1}code/evaluate_minels_v3.py

components=${dir1}components_authors.db
representations=${dir1}representations_authors/representations_v2.db;
features=${dir1}representations_authors/features_v2.db;


python $code $components $representations $features;
