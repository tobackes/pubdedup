INFILE=$1;
OUTFILE=$2;

sqlite3 $OUTFILE "DROP TABLE IF EXISTS surnames; DROP TABLE IF EXISTS firstnames; CREATE TABLE surnames(name TEXT PRIMARY KEY, freq INT); CREATE TABLE firstnames(name TEXT PRIMARY KEY, freq INT)";

sqlite3 $INFILE <<EOF
ATTACH DATABASE "${OUTFILE}" AS outfile;

INSERT INTO outfile.surnames(name,freq)
SELECT surname, COUNT(*) FROM names GROUP BY surname;

INSERT INTO outfile.firstnames(name,freq)
SELECT firstname, COUNT(*) FROM names GROUP BY firstname;
EOF
