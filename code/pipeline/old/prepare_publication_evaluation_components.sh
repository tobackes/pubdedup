dir1=/data_ssd/backests/Repositories/pubdedup/;
dir2=/data_ssds/disk01/backests/pubdedup/;

folder=representations_test5/;

script=code/apply_components_v3.py;

mentions=${dir2}${folder}mentions.db;
representations=${dir2}${folder}representations.db;
features=${dir2}${folder}features.db;

components=${dir1}components_publications_v2.db;

output=${dir1}labelled_publication_mentions.db;

typ=representations;

python $script $mentions $components $features $representations $output $typ;
