import sqlite3
import sys
import time

_db        = sys.argv[1];
_rule_file = sys.argv[2]; #Describes one or more lists of features where for each list, at least one feature must be not NULL

_batchsize = 100000;

con = sqlite3.connect(_db);
cur = con.cursor();

_columns       = [row[1] for row in cur.execute("PRAGMA table_info(representations)")];# if not row[1] in set(['repID','freq'])];
print(_columns)
_questionmarks = ','.join(['?' for i in range(len(_columns))]);
_field2index   = {_columns[i]:i for i in range(len(_columns))};

IN = open(_rule_file);
_restrictions = [line.rstrip().split() for line in IN.readlines()];
IN.close();

def specify(rows,number):
    for row in rows:
        for restriction in _restrictions:
            underspecified = True;
            for field in restriction:
                if row[_field2index[field]]:
                    underspecified = False;
                    break;
            if underspecified:
                row_ = list(row);
                row_[_field2index[restriction[0]]] = '#sep'+str(number);
                number += 1;
                print(row_)
                yield row_,number;
                break;

def representations2specifications(rows_r,number):
    rows_s = list(specify(rows_r,number));
    rows_r = [tuple(row_s) for row_s,number in rows_s];
    return rows_r,(number if len(rows_s)==0 else rows_s[-1][1]);

def update_representations(Q,cur,con):
    number = 0;
    for start,size in Q:
        print(round((start*100.)/num_rows,2),'%'); t=time.time();
        #------------------------------------------------------------------------------------------------------------------------------------
        representations       = cur.execute("SELECT * FROM representations ORDER BY rowid LIMIT ?,?",(start,size,)).fetchall();
        specifications,number = representations2specifications(representations,number); print(time.time()-t,'s for getting representations.'); t=time.time();
        #------------------------------------------------------------------------------------------------------------------------------------
        cur.executemany("INSERT OR REPLACE INTO representations VALUES("+_questionmarks+")",specifications);
        print(time.time()-t,'s for inserting representations.');
        #------------------------------------------------------------------------------------------------------------------------------------
        con.commit(); print(time.time()-t,'s for inserting into index mappings.');
        #------------------------------------------------------------------------------------------------------------------------------------
    return 0;


num_rows = cur.execute("SELECT count(*) FROM representations").fetchall()[0][0];
Q        = [(i*_batchsize,_batchsize,) for i in range(int(num_rows/_batchsize))] + [(num_rows-(num_rows%_batchsize),_batchsize,)];

update_representations(Q,cur,con);

con.close();
