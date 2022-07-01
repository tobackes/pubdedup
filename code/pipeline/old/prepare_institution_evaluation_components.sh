dir1=/data_ssd/backests/Repositories/pubdedup/;
dir2=/data_ssds/disk01/backests/pubdedup/;

folder=representations_institutions_v2/;

script=code/apply_components_v3.py;

mentions=${dir1}${folder}bielefeld.db;
representations=${dir1}${folder}representations.db;
features=${dir1}${folder}features.db;

components=${dir1}components_institutions_bfd.db;

output=${dir1}labelled_institution_mentions.db;

typ=representations;

python $script $mentions $components $features $representations $output $typ;
