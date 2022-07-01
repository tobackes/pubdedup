import sqlite3
import sys
import time
import hashlib

_indb      = sys.argv[1];
_outdb     = sys.argv[2];
_types     = sys.argv[3];
_firstfeat = int(sys.argv[4]);

_batchsize = 1000000;

_max_complen = 4;

_fields = dict();
TYPES   = open(_types);
for line in TYPES:
    feature, rest = line.rstrip().split(':');
    _fields[feature] = rest.split(' ');
TYPES.close();
_featOf  = {field:feature for feature in _fields for field in _fields[feature]};
_columns = set(_featOf.keys());
__fields = [field for feature in _fields for field in _fields[feature]];

#TODO: Check if this works with the new mapping that has institution and division
#_selection     = [2]+[4,5,6,7]+[8,9,10,11]+[12,13,14,15]+[16,17,18,19]+[20,21,22,23]+[24,25,26,27]+[28,29,30,31]+[32,33,34,35]+[36,37,38,39]+[40,41,42,43]+[44,45,46,47]+[48,49,50,51]+[52,53,54,55]+[56,57,58,59]+[60,61,62,63]+[64,65,66,67]+[68,69,70,71]+[72,73,74,75]+[76,77,78,79]+[80,81,82,83]+[84,85,86,87]+[88]+[89]+[90]+[91]+[92]; #TODO: Load this from a mapping file!
#_columns       = ['repID']+['id']+['community1','community2','community3','community4']+['division1','division2','division3','division4']+['none1','none2','none3','none4']+['city1','city2','city3','city4']+['center1','center2','center3','center4']+['faculty1','faculty2','faculty3','faculty4']+['area1','area2','area3','area4']+['institute1','institute2','institute3','institute4']+['academy1','academy2','academy3','academy4']+['university1','university2','university3','university4']+['agency1','agency2','agency3','agency4']+['factory1','factory2','factory3','factory4']+['collection1','collection2','collection3','collection4']+['site1','site2','site3','site4']+['clinic1','clinic2','clinic3','clinic4']+['college1','college2','college3','college4']+['lab1','lab2','lab3','lab4']+['polytechnic1','polytechnic2','polytechnic3','polytechnic4']+['chair1','chair2','chair3','chair4']+['company1','company2','company3','company4']+['association1','association2','association3','association4']+['street']+['number']+['postcode']+['city']+['country'];
_additionals   = []#['street','number','postcode','observed'];
_selectedfield = list(range(_firstfeat,_firstfeat+len(__fields)));                    #TODO: 6 is where the first feature starts, but this is still not general enough
_selection     = list(range(_firstfeat,_firstfeat+len(__fields)+len(_additionals)));  #TODO: 6 is where the first feature starts, but this is still not general enough
_columns       = ['repID','freq']+__fields+_additionals;
_types         = ['TEXT PRIMARY KEY','INT']+['TEXT' for field in __fields]+['TEXT' for additional in _additionals];
_questionmarks = ','.join(['?' for i in range(len(_columns))]);
_ftypes        = ','.join(_columns[2:]);
_groups        = [_selection[i:i+_max_complen] for i in range(0,len(__fields),_max_complen)]+[[el] for el in _selection[len(__fields)+1:]];

def bundle(row): #So what does this do after all? I think sorting and putting the none at the end to get from sets to sequences for hashing!
    row_   = [cell for cell in row];
    for group in _groups:
        grouped  = sorted(list(set([row[i] for i in group if not row[i]==None])));
        grouped += [None for x in range(len(group)-len(grouped))];
        for i in range(len(group)):
            row_[group[i]] = grouped[i];
    return row_;

def make_repID(row):
    #return '+++'.join((str(row_r[x]) for x in _selection));
    return hashlib.sha1('#+*'.join([str(x)+'*+'+str(row[x]) for x in _selection]).encode("utf-8")).hexdigest();

def mentions2representations(rows_m):
    rows_r = [bundle(row_m) for row_m in rows_m];
    repIDs = [make_repID(row_r) for row_r in rows_r];#['+++'.join((str(row_r[x]) for x in _selectedfield)) for row_r in rows_r];
    freqs  = [row_r[3] for row_r in rows_r];
    rows_r = [tuple([repIDs[i],freqs[i]]+[rows_m[i][x] for x in _selection]) for i in range(len(rows_m))];
    return rows_r;

def initialize_primary_keys(cur_in,cur_out,con_out):
    mention        = cur_in.execute("SELECT * FROM mentions ORDER BY mentionID LIMIT 1").fetchall()[0];
    representation = mentions2representations([mention])[0];
    cur_out.execute("INSERT INTO mention2repID   VALUES(?,?)",(-1, None,));
    cur_out.execute("INSERT INTO index2mentionID VALUES(?,?)",( 0, mention[0],));
    cur_out.execute("INSERT INTO index2repID     VALUES(?,?)",( 0, representation[0],));
    con_out.commit();

def insert_representations(Q,cur_in,cur_out,con_out):
    for start,size in Q:
        print(round((start*100.)/num_rows,2),'%'); t=time.time();
        #------------------------------------------------------------------------------------------------------------------------------------
        mentions        = cur_in.execute("SELECT * FROM mentions ORDER BY mentionID LIMIT ?,?",(start,size,)).fetchall();
        representations = mentions2representations(mentions); print(time.time()-t,'s for getting representations.'); t=time.time();
        #------------------------------------------------------------------------------------------------------------------------------------
        cur_out.executemany("INSERT INTO representations(repID,freq,"+_ftypes+") VALUES("+_questionmarks+") ON CONFLICT(repID) DO UPDATE SET freq=freq+?",(list(rep)+[rep[1]] for rep in representations));
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
cur_out.execute("CREATE TABLE index2mentionID(mentionIDIndex INTEGER PRIMARY KEY, mentionID INT  UNIQUE ON CONFLICT IGNORE)");
cur_out.execute("CREATE TABLE index2repID    (repIDIndex     INTEGER PRIMARY KEY, repID     TEXT UNIQUE ON CONFLICT IGNORE)");

num_rows = cur_in.execute("SELECT count(*) FROM mentions").fetchall()[0][0];
Q        = [(i*_batchsize,_batchsize,) for i in range(int(num_rows/_batchsize))] + [(num_rows-(num_rows%_batchsize),_batchsize,)];

initialize_primary_keys(cur_in,cur_out,con_out);
insert_representations(Q,cur_in,cur_out,con_out);

cur_out.execute("CREATE INDEX repID_on_mention_index ON mention2repID(repID)");

con_in.close();
con_out.close();
#-----------------------------------------------------------------------------------------------------------------------
