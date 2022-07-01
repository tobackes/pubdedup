import sqlite3
import sys
import time

_db  = sys.argv[1];

_batchsize = 100000;

_selection     = [1,2]+[3,4,5]+[6,7,8]+[9,10,11]+[12,13,14]+[15,16,17,18]+[19,20,21,22]; #TODO: Load this from a mapping file!
_columns       = ['repID']+['year1','year2']+['a1sur','a1init','a1first']+['a2sur','a2init','a2first']+['a3sur','a3init','a3first']+['a4sur','a4init','a4first']+['term1','term2','term3','term4']+['term1gen','term2gen','term3gen','term4gen'];
_types         = ['TEXT PRIMARY KEY ON CONFLICT IGNORE']+['INT','INT']+['TEXT','TEXT','TEXT']+['TEXT','TEXT','TEXT']+['TEXT','TEXT','TEXT']+['TEXT','TEXT','TEXT']+['TEXT','TEXT','TEXT','TEXT']+['TEXT','TEXT','TEXT','TEXT'];
_questionmarks = ','.join(['?' for i in range(len(_columns[1:]))]);
_ftypes        = ','.join(_columns[1:]);

def is_valid(row):
    if row[2] == None and row[5] == None and row[8] == None and row[11] == None: # no surname
        return False;
    if row[14] == None and row[15] == None and row[16] == None and row[17] == None: # no term
        return False;
    return True;

def generalize(row):
    year_drops      = [set([1]),set([2])];
    name_drops      = [set([3,4,5]),set([6,7,8]),set([9,10,11]),set([12,13,14])];
    term_drops      = [set([15,16,17,18]),set([15,19]),set([16,20]),set([17,21]),set([18,22])];
    year_drops      = [set([drop for drop in year_drop if row[drop] != None]) for year_drop in year_drops];
    name_drops      = [set([drop for drop in name_drop if row[drop] != None]) for name_drop in name_drops];
    term_drops      = [set([drop for drop in term_drop if row[drop] != None]) for term_drop in term_drops];
    year_drops      = [year_drop for year_drop in year_drops if len(year_drop)>0];
    name_drops      = [name_drop for name_drop in name_drops if len(name_drop)>0];
    term_drops      = [term_drop for term_drop in term_drops if len(term_drop)>0];
    yeneralizations = [[row[i] if not i in year_drop else None for i in range(len(row))] for year_drop in year_drops];
    neneralizations = [[row[i] if not i in name_drop else None for i in range(len(row))] for name_drop in name_drops];
    teneralizations = [[row[i] if not i in term_drop else None for i in range(len(row))] for term_drop in term_drops];
    generalizations = [generalization for generalization in yeneralizations+neneralizations+teneralizations if is_valid(generalization)];
    return generalizations;

def bundle(row):
    groups = [[3,6,9,12],[4,7,10,13],[5,8,11,14],[15,16,17,18],[19,20,21,22]]; #TODO: the last group and the second to last need remain in sync!
    row_   = [cell for cell in row];
    for group in groups:
        grouped  = sorted(list(set([row[i] for i in group if not row[i]==None])));
        grouped += [None for x in range(len(group)-len(grouped))];
        for i in range(len(group)):
            row_[group[i]] = grouped[i];
    return row_;

def representations2generalizations(rows_r):
    rows_g = [generalization for row_r in rows_r for generalization in generalize(row_r)];
    rows_r = [bundle(row_g) for row_g in rows_g];
    repIDs = ['+++'.join((str(row_r[x]) for x in _selection)) for row_r in rows_r];
    rows_r = [tuple([repIDs[i]]+[rows_r[i][x] for x in _selection]) for i in range(len(repIDs))];
    return rows_r;

def insert_representations(Q,cur,con):
    for start,size in Q:
        print(round((start*100.)/num_rows,2),'%'); t=time.time();
        #------------------------------------------------------------------------------------------------------------------------------------
        representations = cur.execute("SELECT * FROM representations ORDER BY rowid LIMIT ?,?",(start,size,)).fetchall();
        generalizations = representations2generalizations(representations); print(time.time()-t,'s for getting representations.'); t=time.time();
        #------------------------------------------------------------------------------------------------------------------------------------
        cur.executemany("INSERT INTO representations(repID,"+_ftypes+") VALUES(?,"+_questionmarks+")",generalizations);
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

insert_representations(Q,cur,con);

cur.execute("CREATE INDEX repID_index ON index2repID(repID)");

con.close();

# A type of query that can help understand how to generalize:
# select id,count(*),a1sur,a2sur,a3sur,a4sur from publications where id in (select id from (select id,count(*) as freq from publications where id is not NULL group by id) where freq > 10 limit 0,10) group by id,a1sur,a2sur,a3sur,a4sur;
