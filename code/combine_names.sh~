infolder=$1;
outfile=$2;

sqlite3 $outfile "DROP TABLE IF EXISTS names";

first="";

for file in $infolder*; do
    echo $file;
    filename="${file##*/}";
    filenum="${filename%.*}";
    if [ -z $first ]; then
        first=ok;
        schema=`sqlite3 ${file} "select sql from sqlite_master where type='table' and name='names'"`;
        sqlite3 $outfile "${schema}";
    fi;
    sqlite3 $outfile "ATTACH '${file}' AS temp_${filenum}; INSERT INTO main.names SELECT * FROM temp_${filenum}.names;";
done

while IFS= read -r line; do
    vals=(${line//|/ });
    column=${vals[1]};
    echo $column;
    sqlite3 $outfile "CREATE INDEX ${column}_index ON names(${column})";
done <<< `sqlite3 $outfile "PRAGMA table_info(names);"`;
