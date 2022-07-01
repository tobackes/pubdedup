import sqlite3
import sys

# Repeat with remaining ones that have been written into mentions of this scripts output until none left.

_filenames = sys.stdin.read().rstrip().split(); # expect echo dois/* | python parse_doinfo.py <outfile>
_outfile   = sys.argv[1];


rows = [];
for filename in _filenames:
    IN = open(filename.rstrip());
    for line in IN:
        values = line.rstrip().split();
        rows.append(values);
    IN.close();

missed   = [];
labelled = [];

for i in range(len(rows)):
    if i % 100000 == 0:
        print(rows[i]);
    if len(rows[i]) == 1:
        missed.append(rows[i][0]);
    else:
        try:
            labelled.append([rows[i][0],int(rows[i][2])]);
        except:
            print(rows[i]);
            labelled.append([None,None,404]);

con = sqlite3.connect(_outfile);
cur = con.cursor();

#cur.execute("DROP TABLE IF EXISTS dois");
cur.execute("DROP INDEX IF EXISTS legal_index");
cur.execute("DROP INDEX IF EXISTS code_index");

cur.execute("CREATE TABLE IF NOT EXISTS dois(doi TEXT PRIMARY KEY, legal INT, code INT)");
cur.executemany("INSERT OR REPLACE INTO dois VALUES(?,?,?)",((row[0],row[1]==202 or row[1]==302,row[1],) for row in labelled if row[0]));

con.commit();

cur.execute("CREATE INDEX legal_index ON dois(legal)");
cur.execute("CREATE INDEX code_index  ON dois(code)");

#cur.execute("DROP TABLE IF EXISTS mentions");
#cur.execute("CREATE TABLE mentions(id TEXT PRIMARY KEY, legal INT, code INT)");
#cur.executemany("INSERT INTO mentions VALUES(?,?,?)",((miss,None,None,) for miss in missed));

con.close();
