dir1=/data_ssd/backests/Repositories/pubdedup/;
dir2=/data_ssds/disk01/backests/pubdedup/;

code=${dir1}code/find_components_disk.py

features=${dir2}representations_test5/features.db;

report=${dir1}report_publications_v2.db

components=${dir1}components_publications_v2.db


nice -n 1 python $code $features $report $components;
