import sqlite3
import sys
import time
from itertools import chain, combinations
import hashlib

_db        = sys.argv[1];
_types     = sys.argv[2];
_hierarchy = sys.argv[3];
_rule_file = sys.argv[4]; #Describes one or more lists of features where for each list, at least one feature must be not NULL

_batchsize   = 10000;
_max_complen = 4;

IN = open(_rule_file);
_restrictions = [line.rstrip().split() for line in IN.readlines()];
IN.close();

_fields = dict();
TYPES   = open(_types);
for line in TYPES:
    feature, rest = line.rstrip().split(':');
    _fields[feature] = rest.split(' ');
TYPES.close();

con = sqlite3.connect(_db);
cur = con.cursor();

_columns = [row[1] for row in cur.execute("PRAGMA table_info(representations)")];# if not row[1] in set(['repID','freq'])];

_featOf      = {field:feature for feature in _fields for field in _fields[feature]};
__fields     = [field for feature in _fields for field in _fields[feature]];
_field2index = {_columns[i]:i for i in range(len(_columns))};

_additionals   = [];
_selection     = list(range(2,2+len(__fields)+len(_additionals)));
_questionmarks = ','.join(['?' for i in range(len(_columns))]);
_groups        = [_selection[i:i+_max_complen] for i in range(0,len(__fields),_max_complen)]+[[el] for el in _selection[len(__fields)+1:]];


type_list  = [row[1] for row in cur.execute("PRAGMA table_info(representations)").fetchall()];
_indicesOf = {type_list[i][:-1]:[] for i in range(len(type_list)) if type_list[i][-1] in set([str(x) for x in range(1,_max_complen+1)])};
for i in range(len(type_list)):
    if type_list[i][:-1] in _indicesOf:
        _indicesOf[type_list[i][:-1]].append(i);


_classes = dict();
IN = open(_hierarchy);
for line in IN:
    cls, lvl = line.rstrip().split();
    if lvl in _classes:
        _classes[lvl].append(cls);
    else:
        _classes[lvl] = [cls];
IN.close();

def is_valid(row):
    for restriction in _restrictions:
        underspecified = True;
        for field in restriction:
            if row[_field2index[field]]:
                underspecified = False;
                break;
    return not underspecified;

def powerset(iterable):
    s = list(iterable);
    return chain.from_iterable(combinations(s, r) for r in range(len(s)+1));

def generalize(row): # this is the real, but expensive dropping that creates parent nodes if not observed
    ceneralizations = [[row[i] if not i in class_drop else None for i in range(len(row))] for class_drop in _class_drops];
    generalizations = [generalization for generalization in ceneralizations if is_valid(generalization)];
    return generalizations;

def generalize_(row): # drop everything except id
    inst_drops      = [set([1]),set(range(2,91))];
    inst_drops      = [set([drop for drop in inst_drop if row[drop] != None]) for inst_drop in inst_drops];
    inst_drops      = [inst_drop for inst_drop in inst_drops if len(inst_drop)>0];
    ieneralizations = [[row[i] if not i in inst_drop else None for i in range(len(row))] for inst_drop in inst_drops];
    generalizations = [generalization for generalization in ieneralizations if is_valid(generalization)];
    return generalizations;

def bundle(row):
    groups = _groups;#[[1],[2],[3],[4],[5],[6],[7],[8]];
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
        cur.executemany("INSERT INTO representations VALUES ("+_questionmarks+") ON CONFLICT(repID) DO NOTHING",generalizations);
        print(time.time()-t,'s for inserting representations.');
        #------------------------------------------------------------------------------------------------------------------------------------
        cur.executemany("INSERT INTO index2repID(repID) VALUES(?)",((representation[0],) for representation in generalizations));
        con.commit(); print(time.time()-t,'s for inserting into index mappings.');
        #------------------------------------------------------------------------------------------------------------------------------------
    return 0;


groups          = [[_indicesOf[c] for c in _classes[level]] for level in _classes];
#_class_drops    = [set([el for tup in groups[:i]+[subset] for tup2 in tup for el in tup2]) for i in range(len(groups)) for subset in powerset(groups[i]) if len(subset) > 0 and not(len(subset)==len(groups) and i+1==len(groups))];
_class_drops    = [set([el for j in range(1,len(_groups)) for el in _groups[j] if j!=i]) for i in range(len(_groups))];

num_rows = cur.execute("SELECT count(*) FROM representations").fetchall()[0][0];
Q        = [(i*_batchsize,_batchsize,) for i in range(int(num_rows/_batchsize))] + [(num_rows-(num_rows%_batchsize),_batchsize,)];

#cur.execute("DROP INDEX IF EXISTS repID_index");

insert_representations(Q,cur,con);

#cur.execute("CREATE INDEX repID_index ON index2repID(repID)");

con.close();

# A type of query that can help understand how to generalize:
# select id,count(*),a1sur,a2sur,a3sur,a4sur from publications where id in (select id from (select id,count(*) as freq from publications where id is not NULL group by id) where freq > 10 limit 0,10) group by id,a1sur,a2sur,a3sur,a4sur;
