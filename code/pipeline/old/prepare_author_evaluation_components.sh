dir1=/data_ssd/backests/Repositories/pubdedup/;
dir2=/data_ssds/disk01/backests/pubdedup/;

folder=representations_authors/;

script=code/apply_components_v3.py;

mentions=${dir1}${folder}mentions.db;
representations=${dir1}${folder}representations_v2.db;
features=${dir1}${folder}features_v2.db;

components=${dir1}components_authors.db;

output=${dir1}labelled_author_mentions.db;

typ=authors;

python $script $mentions $components $features $representations $output $typ;
