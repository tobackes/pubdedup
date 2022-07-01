dir1=/data_ssd/backests/Repositories/pubdedup/;
dir2=/data_ssds/disk01/backests/pubdedup/;

code=${dir1}code/find_components_disk.py

features=${dir1}representations_authors/features_v2.db;

report=${dir1}report_authors_v9.db

components=${dir1}components_authors_v9.db


nice -n 1 python $code $features $report $components;
