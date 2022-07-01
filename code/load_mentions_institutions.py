import sqlite3
import sys

_inDB  = sys.argv[1];
_outDB = sys.argv[2];

con_in  = sqlite3.connect(_inDB);
cur_in  = con_in.cursor();
con_out = sqlite3.connect(_outDB);
cur_out = con_out.cursor();

columns = [row[1] for row in cur_in.execute("PRAGMA table_info(representations)").fetchall() if not row[1] in set(['rowid','mentionID','id','string'])];
colstr  = ','.join(columns);

cur_in.execute("SELECT rowid,mentionID,id,1,string,"+colstr+" FROM representations");

cur_out.execute("DROP TABLE IF EXISTS mentions");
cur_out.execute("CREATE TABLE mentions(mentionID INT, originalID TEXT, goldID TEXT, freq REAL, string TEXT,"+colstr+")");

cur_out.executemany("INSERT INTO mentions VALUES("+','.join(['?' for i in range(5+len(columns))])+")",cur_in);

cur_out.execute("CREATE UNIQUE INDEX mentionID_index ON mentions(mentionID)");

con_out.commit();
con_out.close();
con_in.close();
