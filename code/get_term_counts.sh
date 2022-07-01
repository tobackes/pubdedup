INFILE=$1;
OUTFILE=$2;

sqlite3 $OUTFILE "DROP TABLE IF EXISTS terms; CREATE TABLE terms(term TEXT PRIMARY KEY, freq INT);";

sqlite3 $INFILE <<EOF
ATTACH DATABASE "${OUTFILE}" AS outfile;

INSERT INTO outfile.terms(term,freq)
SELECT term, COUNT(*) FROM terms GROUP BY term;
EOF
