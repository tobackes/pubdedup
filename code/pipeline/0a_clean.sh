dir1=/data_ssd/backests/Repositories/pubdedup/;
dir2=/data_ssds/disk01/backests/pubdedup/;


dois=${dir1}resources/dois.db

#sqlite3 ${dir2}representations_publications/mentions_ngram.db "ATTACH DATABASE '${dois}' AS dois; UPDATE mentions SET goldID=NULL WHERE goldID NOT IN (SELECT doi FROM dois.dois WHERE legal) OR goldID IN (SELECT doi FROM dois.types WHERE type NOT IN ('book-chapter','dissertation','monograph','journal-article','proceedings-article','report'));"

sqlite3 ${dir2}representations_publications/mentions_ngrams.db "ATTACH DATABASE '${dois}' AS dois; UPDATE main.mentions SET goldID=NULL WHERE NOT EXISTS(SELECT * FROM legals WHERE doi=goldID);"
