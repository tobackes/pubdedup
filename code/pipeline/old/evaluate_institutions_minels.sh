dir1=/data_ssd/backests/Repositories/pubdedup/;
dir2=/data_ssds/disk01/backests/pubdedup/;

code=${dir1}code/evaluate_minels_v3.py

components=${dir1}components_institutions.db
representations=${dir1}representations_institutions_v2/representations_wos.db;
features=${dir1}representations_institutions_v2/features_wos.db;

echo "python $code $components $representations $features";
nice -n 1 python $code $components $representations $features;
