import sqlite3
import sys
import time

_indb  = sys.argv[1];
_outdb = sys.argv[2];

_batchsize = 1000000;

_selection     = [2]+[4]+[5]+[6]+[7]+[8]+[9]+[10]+[11]; #TODO: Load this from a mapping file!
_columns       = ['repID']+['rid']+['l']+['l_']+['f1']+['f1_']+['f2']+['f2_']+['f3']+['f3_'];
_types         = ['TEXT PRIMARY KEY ON CONFLICT IGNORE']+['TEXT']+['TEXT']+['TEXT']+['TEXT']+['TEXT']+['TEXT']+['TEXT']+['TEXT']+['TEXT'];
_questionmarks = ','.join(['?' for i in range(len(_columns[1:]))]);
_ftypes        = ','.join(_columns[1:]);

def bundle(row):
    groups = [[2],[4],[5],[6],[7],[8],[9],[10],[11]];
    row_   = [cell for cell in row];
    for group in groups:
        grouped  = sorted(list(set([row[i] for i in group if not row[i]==None])));
        grouped += [None for x in range(len(group)-len(grouped))];
        for i in range(len(group)):
            row_[group[i]] = grouped[i];
    return row_;

def mentions2representations(rows_m):
    rows_r = [bundle(row_m) for row_m in rows_m];
    repIDs = ['+++'.join((str(row_r[x]) for x in _selection)) for row_r in rows_r];
    rows_r = [tuple([repIDs[i]]+[rows_r[i][x] for x in _selection]) for i in range(len(rows_m))];
    return rows_r;

def initialize_primary_keys(cur_in,cur_out,con_out):
    mention        = cur_in.execute("SELECT * FROM authors ORDER BY mentionID LIMIT 1").fetchall()[0];
    representation = mentions2representations([mention])[0];
    cur_out.execute("INSERT INTO mention2repID   VALUES(?,?)",(-1, None,));
    cur_out.execute("INSERT INTO index2mentionID VALUES(?,?)",( 0, mention[0],));
    cur_out.execute("INSERT INTO index2repID     VALUES(?,?)",( 0, representation[0],));
    con_out.commit();

def insert_representations(Q,cur_in,cur_out,con_out):
    for start,size in Q:
        print(round((start*100.)/num_rows,2),'%'); t=time.time();
        #------------------------------------------------------------------------------------------------------------------------------------
        mentions        = cur_in.execute("SELECT * FROM authors ORDER BY mentionID LIMIT ?,?",(start,size,)).fetchall();
        representations = mentions2representations(mentions); print(time.time()-t,'s for getting representations.'); t=time.time();
        #------------------------------------------------------------------------------------------------------------------------------------
        cur_out.executemany("INSERT INTO representations(repID,"+_ftypes+") VALUES(?,"+_questionmarks+")",representations);
        print(time.time()-t,'s for inserting representations.');
        #------------------------------------------------------------------------------------------------------------------------------------
        cur_out.executemany("INSERT INTO mention2repID  (repID)     VALUES(?)",((representation[0],) for representation in representations));
        cur_out.executemany("INSERT INTO index2mentionID(mentionID) VALUES(?)",((mention[0]       ,) for mention        in mentions       ));
        cur_out.executemany("INSERT INTO index2repID    (repID)     VALUES(?)",((representation[0],) for representation in representations));
        con_out.commit(); print(time.time()-t,'s for inserting into index mappings.');
        #------------------------------------------------------------------------------------------------------------------------------------
    return 0;

con_in  = sqlite3.connect(_indb);
cur_in  = con_in.cursor();
con_out = sqlite3.connect(_outdb);
cur_out = con_out.cursor();

cur_out.execute("DROP TABLE IF EXISTS representations");
cur_out.execute("DROP TABLE IF EXISTS mention2repID");
cur_out.execute("DROP TABLE IF EXISTS index2mentionID");
cur_out.execute("DROP TABLE IF EXISTS index2repID");
cur_out.execute("CREATE TABLE representations("+', '.join([_columns[i]+' '+_types[i] for i in range(len(_columns))])+")");
cur_out.execute("CREATE TABLE mention2repID  (mentionIDIndex INTEGER PRIMARY KEY, repID     TEXT)");
cur_out.execute("CREATE TABLE index2mentionID(mentionIDIndex INTEGER PRIMARY KEY, mentionID TEXT UNIQUE ON CONFLICT IGNORE)");
cur_out.execute("CREATE TABLE index2repID    (repIDIndex     INTEGER PRIMARY KEY, repID     TEXT UNIQUE ON CONFLICT IGNORE)");

num_rows = cur_in.execute("SELECT count(*) FROM authors").fetchall()[0][0];
Q        = [(i*_batchsize,_batchsize,) for i in range(int(num_rows/_batchsize))] + [(num_rows-(num_rows%_batchsize),_batchsize,)];

initialize_primary_keys(cur_in,cur_out,con_out);
insert_representations(Q,cur_in,cur_out,con_out);

cur_out.execute("CREATE INDEX repID_on_mention_index ON mention2repID(repID)");

con_in.close();
con_out.close();
#-----------------------------------------------------------------------------------------------------------------------
