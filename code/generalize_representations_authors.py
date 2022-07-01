import sqlite3
import sys
import time
import hashlib

_db        = sys.argv[1];

_batchsize = 100000;

_selection     = [2,3,4,5,6,7,8,9]; #TODO: Load this from a mapping file!
_columns       = ['repID','freq','l','l_','f1','f1_','f2','f2_','f3','f3_'];
_questionmarks = ','.join(['?' for i in range(len(_columns))]);

def is_valid(row):
    #if row[1] == None:
    #    return False; # rid required
    #return True;
    #below for nomal mode
    if row[2] == None:                    # no  surname
        return False;
    if row[3] == None and row[4] != None: # first  name but no first  initial
        return False;
    if row[5] == None and row[6] != None: # second name but no second initial
        return False;
    if row[7] == None and row[8] != None: # third  name but no third  initial
        return False;
    return True;

def generalize(row):
    name_drops      = [set([5,6,7,8,9])];
    name_drops      = [set([drop for drop in name_drop if row[drop] != None]) for name_drop in name_drops];
    name_drops      = [name_drop for name_drop in name_drops if len(name_drop)>0];
    neneralizations = [[row[i] if not i in name_drop else None for i in range(len(row))] for name_drop in name_drops];
    generalizations = [generalization for generalization in neneralizations if is_valid(generalization)];
    return generalizations;

def bundle(row):
    groups = [[1],[2],[3],[4],[5],[6],[7],[8]];
    row_   = [cell for cell in row];
    for group in groups:
        grouped  = sorted(list(set([row[i] for i in group if not row[i]==None])));
        grouped += [None for x in range(len(group)-len(grouped))];
        for i in range(len(group)):
            row_[group[i]] = grouped[i];
    return row_;

def make_repID(row):
    #return '+++'.join((str(row_r[x]) for x in _selection));
    return hashlib.sha1('#+*'.join([str(x)+'*+'+str(row[x]) for x in _selection]).encode("utf-8")).hexdigest();

def representations2generalizations(rows_r):
    rows_g = [generalization for row_r in rows_r for generalization in generalize(row_r)];
    rows_r = [bundle(row_g) for row_g in rows_g];
    repIDs = [make_repID(row_r) for row_r in rows_r];#['+++'.join((str(row_r[x]) for x in _selection)) for row_r in rows_r];
    rows_r = [tuple([repIDs[i],0]+[rows_r[i][x] for x in _selection]) for i in range(len(repIDs))];
    return rows_r;

def insert_representations(Q,cur,con):
    for start,size in Q:
        print(round((start*100.)/num_rows,2),'%'); t=time.time();
        #------------------------------------------------------------------------------------------------------------------------------------
        representations = cur.execute("SELECT * FROM representations ORDER BY rowid LIMIT ?,?",(start,size,)).fetchall();
        generalizations = representations2generalizations(representations); print(time.time()-t,'s for getting representations.'); t=time.time();
        #------------------------------------------------------------------------------------------------------------------------------------
        cur.executemany("INSERT INTO representations VALUES("+_questionmarks+") ON CONFLICT(repID) DO NOTHING",generalizations);
        print(time.time()-t,'s for inserting representations.');
        #------------------------------------------------------------------------------------------------------------------------------------
        cur.executemany("INSERT INTO index2repID(repID) VALUES(?)",((representation[0],) for representation in generalizations));
        con.commit(); print(time.time()-t,'s for inserting into index mappings.');
        #------------------------------------------------------------------------------------------------------------------------------------
    return 0;

con = sqlite3.connect(_db);
cur = con.cursor();

num_rows = cur.execute("SELECT count(*) FROM representations").fetchall()[0][0];
Q        = [(i*_batchsize,_batchsize,) for i in range(int(num_rows/_batchsize))] + [(num_rows-(num_rows%_batchsize),_batchsize,)];

#cur.execute("DROP INDEX IF EXISTS repID_index");

insert_representations(Q,cur,con);

#cur.execute("CREATE INDEX repID_index ON index2repID(repID)");

con.close();

# A type of query that can help understand how to generalize:
# select id,count(*),a1sur,a2sur,a3sur,a4sur from publications where id in (select id from (select id,count(*) as freq from publications where id is not NULL group by id) where freq > 10 limit 0,10) group by id,a1sur,a2sur,a3sur,a4sur;
