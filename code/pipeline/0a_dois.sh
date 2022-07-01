dir1=/data_ssd/backests/Repositories/pubdedup/
dir2=/data_ssds/disk01/backests/pubdedup/

script=${dir1}code/parse_doinfo.py
script2=${dir1}code/separate_article_dois_v2.py
script3=${dir1}code/doi_checker.sh

id_file=${dir1}resources/dois.db
doi_folder=${dir1}dois_8/

mentions=${dir2}representations_publications/mentions.db

#sqlite3 $mentions "ATTACH DATABASE '${id_file}' AS dois; INSERT OR IGNORE INTO dois.dois SELECT DISTINCT goldID, NULL, NULL FROM mentions;"

#bash $script3 $id_file $doi_folder

wait

#echo ${doi_folder}*.txt | python $script $id_file

wait

python -i $script2 $id_file
