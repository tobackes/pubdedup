dir1=/data_ssd/backests/Repositories/pubdedup/;
dir2=/data_ssds/disk01/backests/pubdedup/;

code=${dir1}code/find_components_disk.py

features=${dir1}representations_institutions_v2/features_wos.db;

report=${dir1}report_institutions.db

components=${dir1}components_institutions.db


nice -n 1 python $code $features $report $components;
