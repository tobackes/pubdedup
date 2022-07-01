DB=$1

sqlite3 $DB "ALTER TABLE representations RENAME COLUMN string TO freq"
sqlite3 $DB "UPDATE representations SET freq=1.0 WHERE observed=1"
sqlite3 $DB "UPDATE representations SET freq=0.0 WHERE observed=0"
