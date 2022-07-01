infolder=$1;
outfile=$2;

sqlite3 $outfile "DROP TABLE IF EXISTS results";

first="";

for file in $infolder*.db; do
    echo $file;
    filename="${file##*/}";
    filenum="${filename%.*}";
    if [ -z $first ]; then
        first=ok;
        schema=`sqlite3 ${file} "SELECT sql FROM sqlite_master WHERE type='table' AND name='results'"`;
        sqlite3 $outfile "${schema}";
    fi;
    sqlite3 $outfile "ATTACH '${file}' AS temp_${filenum}; INSERT INTO main.results SELECT * FROM temp_${filenum}.results;";
done

while IFS= read -r line; do
    vals=(${line//|/ });
    column=${vals[1]};
    echo $column;
    sqlite3 $outfile "CREATE INDEX ${column}_index ON results(${column})";
done <<< `sqlite3 $outfile "PRAGMA table_info(results);"`;
