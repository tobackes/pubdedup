#IMPORTS--------------------------------------------------------------------------------------------------------------------------------------------------------------------
import sys
import sqlite3
import json
import time
from collections import Counter
from copy import deepcopy as copy
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#GLOBALS--------------------------------------------------------------------------------------------------------------------------------------------------------------------

_key      = sys.argv[1] if sys.argv[1] != 'None' else None;
_value    = sys.argv[2] if sys.argv[2] != 'None' else None;
_outdb    = sys.argv[3];
_cfg_file = sys.argv[4];

_num_spe = False;

_licensing    = True;
_selfprob_thr = 0.075;
_edge_thr_    = 0.01;

cfg_in = open(_cfg_file,'r'); _cfg = json.loads(cfg_in.read()); cfg_in.close();

_max_len_ = 4;
_typeonly = False;
_batch    = 10000;

_excluded = set([]);
_fields   = dict();
TYPES     = open(_cfg['typ_file']);
for line in TYPES:
    feature, rest = line.rstrip().split(':');
    _fields[feature] = rest.split(' ');
TYPES.close();
_featOf = {field:feature for feature in _fields for field in _fields[feature]};

_columns = set(_featOf.keys());

_licenced = dict()

_TIME = {'licenced':0.0,'all':0.0,'in_index2repID':0.0};
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#FUNCTIONS------------------------------------------------------------------------------------------------------------------------------------------------------------------

def load_constraints(filename):
    constraints = {'requires':dict(),'forbids':dict()};
    IN = open(filename);
    for a,typ,b in [line.rstrip().split() for line in IN]:
        d = constraints[['requires','forbids'][typ=='-']];
        if a in d:
            d[a].add(b);
        else:
            d[a] = set([b]);
    IN.close();
    return constraints;

def get_type(node_rep):
    return tuple(sorted([el[0] for el in node_rep]));

def string(node_rep):
    if _cfg['is_names']:
        fields = set([tup[0] for tup in node_rep]);
        return ' '.join([tup[1] for tup in sorted(list(node_rep)) if not((tup[0]=='l' and 'l_' in fields) or (tup[0]=='f1' and 'f1_' in fields) or (tup[0]=='f2' and 'f2_' in fields) or (tup[0]=='f3' and 'f3_' in fields))]);
    node_rep_list = sorted(list(node_rep));
    type2strings = dict();
    for typ, string in node_rep_list:
        if typ in type2strings:
            type2strings[typ].append(string);
        else:
            type2strings[typ] = [string];
    return '\n'.join([typ+': {'+','.join(type2strings[typ])+'}' for typ in type2strings]);

def licenced(node_rep,repID,obsOf,carryOf):
    global _licenced;
    t   = time.time();
    typ = get_type(node_rep);
    if not _licensing:
        _licenced[typ] = True if len(node_rep)>0 and (carryOf==None or (not repID in carryOf) or obsOf[repID]/float(carryOf[repID])>=_selfprob_thr) else False;
    elif len(node_rep)==0 or (carryOf != None and obsOf[repID]/float(carryOf[repID])<_selfprob_thr): #repID in carryOf and
        _licenced[typ] = False;
    if not typ in _licenced:
        maxnumcomp = max([0]+Counter(get_type(node_rep)).values());
        components = set(typ);
        if len(components) == 0 or maxnumcomp > _max_len_: _TIME['licenced'] += time.time()-t; _licenced[typ] = False; return False;
        for component in components:
            if component in _constraints['requires']:
                requirement_fulfilled = False;
                for requirement in _constraints['requires'][component]:
                    if requirement in components:
                        requirement_fulfilled = True;
                        break;
                if not requirement_fulfilled: _TIME['licenced'] += time.time()-t; _licenced[typ] = False; return False;
            if component in _constraints['forbids']:
                for banned in _constraints['forbids'][component]:
                    if banned in components:
                        _TIME['licenced'] += time.time()-t; _licenced[typ] = False; return False;
        _TIME['licenced'] += time.time()-t; _licenced[typ] = True; return True;
    _TIME['licenced'] += time.time()-t;
    return _licenced[typ];

def generalizes(rep1,rep2): #generalizes itself, too
    return len(rep1-rep2)==0;

def oracle(rep1,rep2):
    if generalizes(rep2,rep1):
        return ">";
    elif generalizes(rep1,rep2):
        return "<";
    return None;

def get_representations(cur):
    fields = [field for feature in _fields for field in _fields[feature]];
    if _key == None:
        if _value == None:
            cur.execute("SELECT mentionID, id, "+', '.join(fields)+" FROM publications");
        else:
            cur.execute("SELECT mentionID, id, "+', '.join(fields)+" FROM publications WHERE "+' OR '.join([field+'=?' for field in fields]),tuple([_value for field in fields]));
    else:
        cur.execute("SELECT mentionID, id, "+', '.join(fields)+" FROM publications WHERE "+' OR '.join([[_key+"=?"],[_key+str(i)+"=?" for i in xrange(1,_max_len_+1)]][_key in _fields_]),tuple([[_value],[_value for i in xrange(1,_max_len_+1)]][_key in _fields_]));
    ID2rep = dict();
    ID2obs = dict();
    page = 0;
    while True:
        rows    = cur.fetchmany(_batch);
        if len(rows) == 0: break;
        for row in rows:
            list_rep = row[2:];
            rep      = set([(_featOf[fields[i]],'',) for i in xrange(len(fields)) if list_rep[i] != None]) if _typeonly else set([(_featOf[fields[i]],list_rep[i],) for i in xrange(len(fields)) if list_rep[i] != None]);#TODO: This does not support _special fields yet
            repID    = string(rep);
            if not repID in ID2rep:
                ID2rep[repID] = rep;
                ID2obs[repID] = 1;
            else:
                ID2obs[repID] += 1;
        page += 1;
        if page%1==0: printer(page);
    legals, illegals = set([]), set([]);
    for repID,rep in ID2rep.iteritems():
        if licenced(rep,repID,None,None):
            legals.add(repID);
        else:
            illegals.add(repID);
    return legals, illegals, ID2rep, ID2rep.keys(), ID2obs;

def generalizations(spe_rep,gens,ID2obs,ID2car):
    for el in spe_rep:
        gen_rep = spe_rep - set([el]);
        gen_str = string(gen_rep);
        if not licenced(gen_rep,gen_str,ID2obs,ID2car): continue;
        if not gen_str in gens:
            gens.add(gen_str);
            yield gen_str;
            for x in generalizations(gen_rep,gens,ID2obs,ID2car):
                yield x;

def generalizations_(spe_rep,gens,ID2obs,ID2car):
    spe_str = string(spe_rep);
    for el in spe_rep:
        gen_rep = spe_rep - set([el]);
        gen_str = string(gen_rep);
        if ID2car[spe_str]/float(ID2car[gen_str])<_edge_thr_: continue;
        if not gen_str in gens:
            gens.add(gen_str);
            yield gen_str;
            for x in generalizations_(gen_rep,gens,ID2obs,ID2car):
                yield x;

def find_min_els(repIDs,ID2rep,ID2obs,ID2car):
    min_els = {repID: [1,ID2obs[repID]] for repID in set(repIDs)}; page = 0;
    for x in repIDs:
        for genID in generalizations(ID2rep[x],set([]),ID2obs,ID2car):
            if genID in min_els:
                del min_els[x];
                min_els[genID][0] += 1;
                min_els[genID][1] += ID2obs[x];
                break;
        page += 1;
        if page%1==0: printer(page);
    min_els_l = min_els.keys();
    return min_els_l, {repID: min_els[repID][0] for repID in min_els_l}, {repID: min_els[repID][1] for repID in min_els_l};

def printer(something):
    sys.stdout.write('\r\x1b[K');
    sys.stdout.write(str(something));
    sys.stdout.flush();

#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#SCRIPT---------------------------------------------------------------------------------------------------------------------------------------------------------------------
printer('Connecting to databases...\n');
#-------------------------------------------------------------------------------------------------------------------------------------
_constraints = load_constraints(_cfg['con_file']);
con,con_out  = sqlite3.connect(_cfg['root_dir']+_cfg['name_db']), sqlite3.connect(_outdb);
cur,cur_out  = con.cursor(), con_out.cursor();
#-------------------------------------------------------------------------------------------------------------------------------------
printer('Loading publications...\n');
#-------------------------------------------------------------------------------------------------------------------------------------
legals, illegals, repID2rep, index2repID, repID2obs = get_representations(cur);
repID2index                                         = {index2repID[i]:i for i in xrange(len(index2repID))};
#-------------------------------------------------------------------------------------------------------------------------------------
printer('Finding mimimal elements...\n');
#-------------------------------------------------------------------------------------------------------------------------------------
min_els, specsOf, carryOf = find_min_els(legals,repID2rep,repID2obs,None); # The carryOf is only defined for min_els!
#-------------------------------------------------------------------------------------------------------------------------------------
legal_min_els   = set([ repID for repID in min_els if     licenced(repID2rep[repID],repID,repID2obs,carryOf) ]);
illegal_min_els = set([ repID for repID in min_els if not licenced(repID2rep[repID],repID,repID2obs,carryOf) ]);
#-------------------------------------------------------------------------------------------------------------------------------------
#TODO: I think there is a bug here because if one generalization is illegal, the not necessarily are all its generalizations.
#      We could get minimal elements that include other minimal elements because the path is blocked to find them?
while True:
    min_els_, specsOf_, carryOf_ = find_min_els(legals-illegal_min_els,repID2rep,repID2obs,carryOf);
    diff                         = len(set(min_els)|set(min_els_)) - len(set(min_els)&set(min_els_));
    print '\nNew:', len(set(min_els_)-set(min_els)); print 'Old:', len(set(min_els)-set(min_els_));
    carryOf.update(carryOf_); specsOf.update(specsOf_); min_els=copy(min_els_); print 'min-el difference:', diff;
    if diff==0: break; #TODO: now its just steady...
    legal_min_els_   = set([ repID for repID in min_els if     licenced(repID2rep[repID],repID,repID2obs,carryOf) ]);
    illegal_min_els_ = set([ repID for repID in min_els if not licenced(repID2rep[repID],repID,repID2obs,carryOf) ]);
    legal_min_els    = (legal_min_els-illegal_min_els_) | legal_min_els_;
    illegal_min_els  = (illegal_min_els-legal_min_els_) | illegal_min_els_;
#-------------------------------------------------------------------------------------------------------------------------------------
printer('Creating output tables...\n');
#-------------------------------------------------------------------------------------------------------------------------------------
cur_out.execute("DROP   TABLE IF EXISTS queries");
cur_out.execute("CREATE TABLE           queries(queryID INTEGER PRIMARY KEY AUTOINCREMENT, query TEXT UNIQUE, repIDIndex INT UNIQUE, repID TEXT UNIQUE, num_spe INT, car INT, obs INT, legal BOOL)");
#-------------------------------------------------------------------------------------------------------------------------------------
printer('Writing output for minimal elements...\n');
#-------------------------------------------------------------------------------------------------------------------------------------
page = 0;
for repID in legal_min_els:    #if repID == set([]): continue;
    rep         = repID2rep[repID];
    repIDIndex  = repID2index[repID];
    obs         = repID2obs[repID];
    car         = carryOf[repID];
    num_spe     = specsOf[repID];
    rep_        = [[('a'+str(i)+tup[0],tup[1],) for i in xrange(1,_max_len_+1)] for tup in rep];
    query       = "SELECT mentionID FROM publications WHERE "+' AND '.join(['('+(' OR '.join([el[0]+'="'+el[1]+'"' for el in part]))+')' for part in rep_]);
    cur_out.execute("INSERT INTO queries(query,repIDIndex,repID,num_spe,car,obs,legal) VALUES(?,?,?,?,?,?,?)",(query,repIDIndex,repID,num_spe,car,obs,True,)); con_out.commit();
    page += 1;
    if page%100==0: printer(page);
#-------------------------------------------------------------------------------------------------------------------------------------
printer('Writing output for illegal elements...\n');
#-------------------------------------------------------------------------------------------------------------------------------------
page = 0;
for repID in illegals|illegal_min_els:    #if repID == set([]): continue;
    rep        = repID2rep[repID];
    repIDIndex = repID2index[repID];
    obs        = repID2obs[repID];
    rep_       = [[('a'+str(i)+tup[0],tup[1],) for i in xrange(1,_max_len_+1)] for tup in rep];
    involved   = set([el[0] for el in rep_]);
    missing    = _columns - involved;
    suffix     = ' AND '+' AND '.join([miss+'=NULL' for miss in missing]);
    query      = "SELECT mentionID FROM publications WHERE "+' AND '.join(['('+(' OR '.join([el[0]+'="'+el[1]+'"' for el in part]))+')' for part in rep_])+suffix;
    cur_out.execute("INSERT INTO queries(query,repIDIndex,repID,num_spe,car,obs,legal) VALUES(?,?,?,?,?,?,?)",(query,repIDIndex,repID,1,obs,obs,False,)); con_out.commit();
    page += 1;
    if page%100==0: printer(page);
#-------------------------------------------------------------------------------------------------------------------------------------
printer('Closing connections...\n');
#-------------------------------------------------------------------------------------------------------------------------------------
con.close(); con_out.close();
#-------------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------
