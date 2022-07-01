import sqlite3
import sys
import time

_inDB  = sys.argv[1];
_outDB = sys.argv[2];
_types = sys.argv[3];

_batch  = 1000;

_constraint = "a1sur IS NOT NULL AND term1 IS NOT NULL";

_fields = dict();
TYPES   = open(_types);
for line in TYPES:
    feature, rest = line.rstrip().split(':');
    _fields[feature] = rest.split(' ');
TYPES.close();
_featOf  = {field:feature for feature in _fields for field in _fields[feature]};
_columns = set(_featOf.keys());
__fields = [field for feature in _fields for field in _fields[feature]];

con   = sqlite3.connect(_inDB);
cur   = con.cursor();
con_w = sqlite3.connect(_outDB);
cur_w = con_w.cursor();

cur_w.execute("DROP   TABLE IF EXISTS features");
cur_w.execute("DROP   TABLE IF EXISTS index2feat");
cur_w.execute("DROP   TABLE IF EXISTS index2mentionID");
cur_w.execute("CREATE TABLE           features       (mentionIDIndex INT,                 featIndex INT)");
cur_w.execute("CREATE TABLE           index2feat     (featIndex      INTEGER PRIMARY KEY, featGroup TEXT, feat TEXT)");
cur_w.execute("CREATE TABLE           index2mentionID(mentionIDIndex INTEGER PRIMARY KEY, mentionID TEXT)");

mentionID2index = dict();
grp_expression  = ','.join([featname for group in _fields for featname in _fields[group]]);
sum_expression  = '('+')+('.join([featname+' IS NULL' for group in _fields for featname in _fields[group]])+')'; print(sum_expression);
cur.execute("SELECT mentionID FROM publications WHERE "+_constraint+" GROUP BY "+grp_expression+" ORDER BY "+sum_expression+" DESC");
for row in cur:
    length                  = len(mentionID2index);
    mentionID2index[row[0]] = length;
    if length % 1000000 == 0: print(length);

print('writing index2mentionID...');
cur_w.executemany("INSERT INTO index2mentionID VALUES(?,?)"  ,((mentionID2index[mentionID],      mentionID,) for mentionID in mentionID2index));
con_w.commit();

offset = 0;
for group in _fields:
    print(group);
    feat2index = dict();
    length     = 0;
    for featname in _fields[group]:
        print(featname);
        cur.execute("SELECT "+featname+" FROM publications WHERE "+featname+" IS NOT NULL AND "+_constraint);
        for row in cur:
            feat = row[0];
            if not feat in feat2index:
                feat2index[feat] = offset + length;
                length          += 1;
                if length % 100000 == 0: print(length);
    offset += length;
    cur.execute("SELECT "+','.join(['mentionID']+_fields[group])+" FROM publications WHERE "+_constraint);
    print('writing',group,'...');
    cur_w.executemany("INSERT INTO features        VALUES(?,?)"  ,((mentionID2index[row[0]],feat2index[feat],  ) for row       in cur for feat in row[1:] if feat != None));
    print('writing index2feat...');
    cur_w.executemany("INSERT INTO index2feat      VALUES(?,?,?)",((feat2index[feat],          group,feat,     ) for feat      in feat2index));
    con_w.commit();

con.close();

print('creating index on mentionIDIndex...');
cur_w.execute("CREATE INDEX mentionIDindex_index ON features(mentionIDIndex)");
print('creating index on featIndex...');
cur_w.execute("CREATE INDEX featIndex_index ON features(featIndex)");

cur_w.execute("CREATE TABLE mentions(mentionIDIndex INTEGER PRIMARY KEY, size INT)");
cur_w.execute("INSERT INTO mentions(mentionIDIndex,size) SELECT mentionIDIndex, COUNT(featIndex) FROM features GROUP BY mentionIDIndex");
cur_w.execute("CREATE INDEX size_index ON mentions(size)");

con_w.close();
