#IMPORTS--------------------------------------------------------------------------------------------------------------------------------------------------------------------
import sys
import sqlite3
import json
import time
import multiprocessing as MP
import numpy as np
from collections import Counter
from copy import deepcopy as copy
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#GLOBALS--------------------------------------------------------------------------------------------------------------------------------------------------------------------

_key       = sys.argv[1] if sys.argv[1] != 'None' else None;
_value     = sys.argv[2] if sys.argv[2] != 'None' else None;
_outdb     = sys.argv[3];
_cfg_file  = sys.argv[4];
_jobs      = int(sys.argv[5]);
_jobs_feed = 4;

_minels = True;

cfg_in = open(_cfg_file,'r'); _cfg = json.loads(cfg_in.read()); cfg_in.close();

_max_len_    = 4;
_typeonly    = False;

_queues           = int(1+(_jobs/8));
_batch            = 10000; # The larger the batch, the longer the put() and get() locks
_max_trytime      = _jobs / 16;
_sleeptime        = 0.001*_jobs;
_sleeptime_feeder = 0.0001*_jobs;
_wait             = _batch/10000;
_wait_time        = 0.01*_jobs;
_limit            = 10000000;

_excluded = set([]);
_special  = set([]);
_fields   = dict();
TYPES     = open(_cfg['typ_file']);
for line in TYPES:
    feature, rest = line.rstrip().split(':');
    _fields[feature] = rest.split(' ');
TYPES.close();
_featOf = {field:feature for feature in _fields for field in _fields[feature]};

_columns = set(_featOf.keys());

__fields = [field for feature in _fields for field in _fields[feature]];
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#FUNCTIONS------------------------------------------------------------------------------------------------------------------------------------------------------------------

def printer(something):
    sys.stdout.write('\r\x1b[K');
    sys.stdout.write(str(something));
    sys.stdout.flush();

def unique_indeces(seq):
    seen = set()
    seen_add = seen.add
    return [i for i in range(len(seq)) if not (seq[i] in seen or seen_add(seq[i]))];

def get_type(node_rep):
    return tuple(sorted([el[0] for el in node_rep]));

def string_(node_rep):
    if _cfg['is_names']:
        fields = set([tup[0] for tup in node_rep]);
        return ' '.join([tup[1] for tup in sorted(list(node_rep)) if not((tup[0]=='l' and 'l_' in fields) or (tup[0]=='f1' and 'f1_' in fields) or (tup[0]=='f2' and 'f2_' in fields) or (tup[0]=='f3' and 'f3_' in fields))]);
    node_rep_list = sorted(list(node_rep));
    type2strings  = dict();
    for typ, string in node_rep_list:
        string_ = string if isinstance(string,str) else str(string);
        if typ in type2strings:
            type2strings[typ].append(string_);
        else:
            type2strings[typ] = [string_];
    return '\n'.join([typ+': {'+','.join(type2strings[typ])+'}' for typ in type2strings]);

def string(node_rep):
    if _cfg['is_names']:
        fields = set([tup[0] for tup in node_rep]);
        return ' '.join([tup[1] for tup in sorted(list(node_rep)) if not((tup[0]=='l' and 'l_' in fields) or (tup[0]=='f1' and 'f1_' in fields) or (tup[0]=='f2' and 'f2_' in fields) or (tup[0]=='f3' and 'f3_' in fields))]);
    node_rep_list = sorted(list(node_rep));
    type2strings  = dict();
    for typ, string in node_rep_list:
        string_ = '"'+string+'"' if isinstance(string,str) else '"'+str(string)+'"';
        if typ in type2strings:
            type2strings[typ].append(string_);
        else:
            type2strings[typ] = [string_];
    return '{'+','.join(['"'+typ+'"'+':['+','.join(type2strings[typ])+']' for typ in type2strings])+'}';

def generalizes(rep1,rep2): #generalizes itself, too
    return len(rep1-rep2)==0;

def connected_components(repIDs,reps,ids,parent=None,number=None,size2indeces=None,el2sets=None):
    #- PREPARATION ONLY -------------------------------------
    #indeces      = unique_indeces(repIDs);
    #repIDs       = [repIDs[i] for i in indeces];
    #reps         = [reps[i]   for i in indeces];
    if size2indeces == None:
        size2indeces = dict();
        el2sets      = dict();
        for i in range(len(repIDs)):
            size = len(reps[i]);
            if size in size2indeces:
                size2indeces[size].add(i);
            else:
                size2indeces[size] = set([i]);
                el2sets[size]      = dict();
            for el in reps[i]:
                if el in el2sets[size]:
                    el2sets[size][el].add(i);
                else:
                    el2sets[size][el] = set([i]);
    #--------------------------------------------------------
    #- THE ACTUAL SEARCH ------------------------------------
    parent     = list(range(len(repIDs))) if parent == None else parent;
    number     = [1 for i in range(len(repIDs))] if number == None else number;
    unassigned = {size: set(size2indeces[size]) for size in size2indeces};
    for size in sorted(unassigned.keys()):
        while not len(unassigned[size]) == 0:
            i              = unassigned[size].pop();
            if not licenced(reps[i],repIDs[i]): continue; #TODO: This might no be enough!
            specifications = set([]);
            for size_ in [size__ for size__ in el2sets if size__ > size]:
                els             = [el for el in reps[i] if el in el2sets[size_]];
                specifications |= set([]) if len(els) != len(reps[i]) else set.intersection(*[el2sets[size_][el] for el in els]);
            for j in specifications:
                #------------------------ set_i = FIND(i)
                set_i,z = None,i;
                while True:
                    set_i = parent[z];
                    if set_i == z: break;
                    z = set_i;
                #------------------------ set_j = FIND(j)
                set_j,z = None,j;
                while True:
                    set_j = parent[z];
                    if set_j == z: break;
                    z = set_j;
                #------------------------ UNION(set_i,set_j)
                if set_i != set_j:
                    wini        = number[set_i] >= number[set_j];
                    fro         = [set_i,set_j][wini];
                    to          = [set_j,set_i][wini];
                    parent[fro] = to;
                    number[to] += number[fro];
                unassigned[len(reps[j])] -= set([j]);
    #------------------------------------ COMPRESS(parent)
    for i in range(len(parent)):
        par,z,num = parent[i],i,number[i];
        while par != z:
            number[par] -= num;
            z            = par;
            par = parent[z];
            if par == z:
                number[par] += num;
        parent[i] = par;
    return repIDs, reps, ids, parent, number, size2indeces, el2sets;

def make_result(repIDs,reps,ids,parent,show=False):
    result = dict();
    for i in range(len(parent)):
        if parent[i] in result:
            result[parent[i]].append((repIDs[i],reps[i],ids[i],));
        else:
            result[parent[i]] = [(repIDs[i],reps[i],ids[i],)];
    if show:
        print(Counter(Counter(parent).values()));
        for label in result:
            if len(result[label]) > 1 and len(result[label]) < 10:
                print('#########################');
                for s in result[label]:
                    print(s); print('----------------------');
    return result;

def merge(d, u):
    for k, v in u.items():
        if (not k in d) or d[k] == None:
            d[k] = v;
        elif isinstance(v,dict) and v != {}:
            d[k] = merge(d.get(k,{}),v);
        elif isinstance(v,set):
            d[k] = d[k] | v;
        elif isinstance(v,list):
            d[k] = d[k] + v;
        elif isinstance(v,int) or isinstance(v,float):
            d[k] = d[k] + v;
        elif v != dict():
            d[k] = v;
    return d;

def combine(repIDss,repss,idss,parents,numbers,size2indecess,el2setss):
    offset       = len(parents[0]);
    parent       = parents[0] + [index+offset for index in parents[1]];
    number       = numbers[0] + numbers[1];
    repIDs       = repIDss[0] + repIDss[1];
    reps         = repss[0]   + repss[1];
    ids          = idss[0]    + idss[1];
    sizes        = sorted((list(set(size2indecess[0].keys())|set(size2indecess[1].keys()))));
    size2indeces = {size:size2indecess[0][size] if size in size2indecess[0] else set()  for size in sizes};
    el2sets      = {size:el2setss[0][size]      if size in el2setss[0]      else dict() for size in sizes};
    for size in size2indecess[1]:
        size2indeces[size] |= set([offset+index for index in size2indecess[1][size]]);
        el2sets[size]       = merge(el2sets[size],{el:set([index+offset for index in el2setss[1][size][el]]) for el in el2setss[1][size]});
    return repIDs, reps, ids, parent, number, size2indeces, el2sets;

def find_min_els(repIDs,reps,ids):
    #- PREPARATION ONLY -------------------------------------
    indeces      = unique_indeces(repIDs);
    repIDs       = [repIDs[i] for i in indeces];
    reps         = [reps[i]   for i in indeces];
    ids          = [ids[i]    for i in indeces];
    size2indeces = dict();
    el2sets      = dict();
    for i in range(len(repIDs)):
        size = len(reps[i]);
        if size in size2indeces:
            size2indeces[size].add(i);
        else:
            size2indeces[size] = set([i]);
            el2sets[size]      = dict();
        for el in reps[i]:
            if el in el2sets[size]:
                el2sets[size][el].add(i);
            else:
                el2sets[size][el] = set([i]);
    #--------------------------------------------------------
    #- THE ACTUAL SEARCH ------------------------------------
    min_els = set(range(len(repIDs)));
    for size in sorted(size2indeces.keys()):
        for i in size2indeces[size]:
            if not licenced(reps[i],repIDs[i]): continue;
            specifications = set([]);
            for size_ in [size__ for size__ in el2sets if size__ > size]:
                els             = [el for el in reps[i] if el in el2sets[size_]];
                specifications |= set([]) if len(els) != len(reps[i]) else set.intersection(*[el2sets[size_][el] for el in els]);
            min_els       -= specifications;
    min_els = sorted(list(min_els));
    #--------------------------------------------------------
    return [repIDs[i] for i in min_els], [reps[i] for i in min_els], [ids[i] for i in min_els];

def licenced(rep,repID): #TODO: This is very expensive in the long run!
    types = set([el[0] for el in rep]);
    if 'title' in types and 'surname' in types:
        return True;
    #for el in rep:
    #    if el[0]=='title':# and el[1] != None:
    #        return True;
    return False;

def generalizations(spe_rep,gens):
    for el in spe_rep:
        gen_rep = spe_rep - set([el]);
        gen_str = string(gen_rep);
        if not licenced(gen_rep,gen_str): continue;
        if not gen_str in gens:
            gens.add(gen_str);
            yield gen_str;
            for x in generalizations(gen_rep,gens):
                yield x;

def put(queues,job):
    queue_index = None;
    start_time  = time.time();
    try_time    = 0;
    while True:
        sizes       = [queue.qsize() for queue in queues]; print(sizes);
        queue_index = np.argmin(sizes);#np.random.randint(len(queues));#
        try:
            print('...trying put...');
            queues[queue_index].put(job,block=False);
            print('...done put...');
            break;
        except:
            try_time = time.time() - start_time;
            time.sleep(_sleeptime_feeder);
            pass;

def work(queues,R,x,ROWS=True):
    while True:
        rowss = [None,None];
        #---------------------------------------------------------------------------------------------------
        sizes = [queue.qsize() for queue in queues];
        print(sizes);
        #--------------------------------------------------------------------------------------------------- #TODO: Generalize for more than 2
        queue_index_1 = None;
        start_time    = time.time();
        try_time      = 0;
        while True:
            sizes         = [queue.qsize() for queue in queues];
            queue_index_1 = np.argmax(sizes);#np.random.randint(len(queues));#
            try:
                print('...trying get...');
                rowss[0] = queues[queue_index_1].get(block=False);
                break;
            except:
                try_time = time.time() - start_time;
                if try_time > _max_trytime:
                    print('Closing job', x);
                    return 1;
                time.sleep(_sleeptime);
                pass;
        #--------------------------------------------------------------------------------------------------- #TODO: Generalize for more than 2
        queue_index_2 = None;
        start_time    = time.time();
        try_time      = 0;
        while True:
            sizes         = [queue.qsize() for queue in queues];
            queue_index_2 = np.argmax(sizes);#np.random.randint(len(queues));#
            try:
                print('...trying get...');
                rowss[1] = queues[queue_index_2].get(block=False);
                break;
            except:
                try_time = time.time() - start_time;
                if try_time > _max_trytime:
                    print('Worker',x,'says: Could not get second item, put first one back');
                    put(queues,rowss[0]);
                    print('Closing job', x);
                    return 1;
                time.sleep(_sleeptime);
                pass;
        #--------------------------------------------------------------------------------------------------- #TODO: Generalize for more than 2
        if _minels:
            repIDss, repss, idss = [None,None],[None,None],[None,None];
            for j in range(len(rowss)):
                repIDss[j],repss[j],idss[j] = rowss[j];
        else:
            repIDss, repss, idss, parents, numbers, size2setss, el2setss = [None,None],[None,None],[None,None],[None,None],[None,None],[None,None],[None,None];
            for j in range(len(rowss)):
                repIDss[j], repss[j], idss[j], parents[j], numbers[j], size2setss[j], el2setss[j] = rowss[j];
        #--------------------------------------------------------------------------------------------------- #TODO: Generalize for more than 2
        if _minels:
            min_el_IDss  = [[],[]]; # the representation-based ids
            min_el_repss = [[],[]];
            min_el_idss  = [[],[]]; # the GWS ids
            for j in range(len(repIDss)):
                print('Worker',x,j,'says: "Searching for minels in:', len(repIDss[j]), ' overall elements"');
                #min_el_IDss[j], min_el_repss[j] = find_min_els(repIDss[j],repss[j]);
                #------------------------------------------------------------------------------------------
                repIDs_, reps_, ids_, parent_, number_, size2indeces_, el2sets_ = connected_components(repIDss[j],repss[j],idss[j]);
                result                                                          = make_result(repIDs_,reps_,ids_,parent_);
                for label in result:
                    min_el_IDs, min_el_reps, min_el_ids = find_min_els([repID for repID,rep,ID in result[label]],[rep for repID,rep,ID in result[label]],[ID for repID,rep,ID in result[label]]);
                    min_el_IDss[j]  += min_el_IDs;
                    min_el_repss[j] += min_el_reps;
                    min_el_idss[j]  += min_el_ids;
                print('Worker',x,j,'says: "Number of minels found:', len(min_el_IDss[j]), '"');
        else:
            results = [None,None];
            for j in range(len(repIDss)):
                t = time.time();
                print('Worker',x,j,'says: "Searching for components in:', len(repIDss[j]), ' overall elements"');
                results[j] = connected_components(repIDss[j],repss[j],idss[j],parents[j],numbers[j],size2setss[j],el2setss[j]);
                print('Worker',x,j,'says: "Number of components found:', len(set(results[j][2])), Counter(Counter(results[j][3]).values()), ' -- took',time.time()-t,'sec"');
        #---------------------------------------------------------------------------------------------------
        if _minels:
            sizes       = [queue.qsize() for queue in queues];
            queue_index = np.argmin(sizes);
            print('Worker',x,'says: "Making new job with', len(min_el_IDss[0]),'+',len(min_el_IDss[1]),'elements"');
            queues[queue_index].put((min_el_IDss[0]+min_el_IDss[1],min_el_repss[0]+min_el_repss[1],min_el_idss[0]+min_el_idss[1],));
        else:
            t = time.time();
            combination = tuple(combine(*zip(*results)));
            print('Worker',x,'says: "Made new job with', len(set(results[0][2])),'+',len(set(results[1][2])),'components -- took',time.time()-t,'sec"'); t = time.time();
            put(queues,combination);
            print('Worker',x,'says: "Put new job into queue -- took',time.time()-t,'sec"');
        #---------------------------------------------------------------------------------------------------
    return 1;

def fetch(Q):
    con = sqlite3.connect(_cfg['root_dir']+_cfg['name_db']);
    cur = con.cursor();
    if _key == None:
        if _value == None:
            cur.execute("SELECT mentionID, id, freq, "+', '.join(__fields)+" FROM publications LIMIT "+str(_limit));# ORDER BY RANDOM()");
        else:
            cur.execute("SELECT mentionID, id, freq, "+', '.join(__fields)+" FROM publications WHERE "+' OR '.join([field+'=?' for field in __fields])+' ORDER BY RANDOM()',tuple([_value for field in __fields]));
    elif _key == 'query':
        cur.execute("SELECT mentionID, id, freq, "+', '.join(__fields)+" FROM publications WHERE mentionID IN "'('+_value+') ORDER BY RANDOM()');
    elif _key == 'bielefeld':
        cur.execute("SELECT mentionID, id, freq, "+', '.join(__fields)+" FROM publications WHERE id IN "'('+_value+') ORDER BY RANDOM()');
    else:
        print("SELECT mentionID, id, freq, "+', '.join(__fields)+" FROM publications WHERE "+' OR '.join([[_key+"=?"],[_key+str(i)+"=?" for i in range(1,_max_len_+1)]][_key not in set(_fields.keys())-_special])+' ORDER BY RANDOM()');
        cur.execute("SELECT mentionID, id, freq, "+', '.join(__fields)+" FROM publications WHERE "+' OR '.join([[_key+"=?"],[_key+str(i)+"=?" for i in range(1,_max_len_+1)]][_key not in set(_fields.keys())-_special])+' ORDER BY RANDOM()',tuple([[_value],[_value for i in range(1,_max_len_+1)]][_key not in set(_fields.keys())-_special]));
    while True:
        rows   = cur.fetchmany(_batch);
        if len(rows) == 0:
            print('Closing fetcher...'); break;
        Q.put(rows);
    con.close();
    return 1;

def feed(Q,queues,x): #TODO: DOES NOT JOIN CURRENTLY!
    page = 0;
    while True:
        t          = time.time();
        rows       = None;
        start_time = time.time();
        try_time   = 0;
        while True:
            try:
                rows = Q.get(timeout=120); #Probably this does not fail but blocks
                print('Queue size with batches:',Q.qsize());
                break;
            except:
                try_time = time.time() - start_time;
                if try_time > _max_trytime:
                    print('Closing feeder', x);
                    return 1;
                time.sleep(_sleeptime_feeder);
                pass;
        repIDs   = [];
        reps     = [];
        ids      = [];
        t_fetch  = time.time() - t; t = time.time();
        for row in rows:
            mentionID = row[0];
            dupID     = row[1];
            freq      = row[2];
            list_rep  = row[3:];
            rep       = set([(_featOf[__fields[i]],'',) for i in range(len(__fields)) if list_rep[i] != None]) if _typeonly else set([(_featOf[__fields[i]],list_rep[i],) for i in range(len(__fields)) if list_rep[i] != None]);
            repID     = string(rep);
            repIDs.append(repID);
            reps.append(rep);
            ids.append((dupID,freq,));
        job         = (repIDs,reps,ids,) if _minels else (repIDs,reps,ids,None,None,None,None);
        t_prepare   = time.time() - t; t = time.time();
        put(queues,job);
        page += 1;
        t_put = time.time() - t;
        print(page, t_fetch, t_prepare, t_put,'secs for fetching / preparing / feeding one batch.');
        #while queues[-1].qsize() > int(_jobs*4 / _queues):
        #    time.sleep(0.5);
    return 1;

def write_dups(result,outfile):
    duplicateset = [];
    min_els      = [];
    for i in result:
        dups                          = set([el[2][0] for el in result[i] if el[2][1] > 0]);
        min_repIDs, min_reps, min_ids = find_min_els([el[0] for el in result[i]],[el[1] for el in result[i]],[el[2][0] for el in result[i]]);
        if len(dups) > 1:
            duplicateset.append(dups);
            min_els.append(set(min_repIDs));
    if outfile.endswith('.txt'):
        OUT = open(outfile,'w');
        OUT.write('\n---\n'.join(('\n'.join((dup for dup in dups)) for dups in duplicateset)));
        OUT.close();
    elif outfile.endswith('.db'):
        con = sqlite3.connect(outfile);
        cur = con.cursor();
        cur.execute('DROP TABLE IF EXISTS labelling');
        cur.execute('DROP TABLE IF EXISTS min_els');
        cur.execute('CREATE TABLE labelling(id TEXT PRIMARY KEY,  label INT)');
        cur.execute('CREATE TABLE min_els(repID TEXT PRIMARY KEY, label INT)');
        cur.executemany('INSERT OR IGNORE INTO labelling VALUES(?,?)',((dup,i,) for i in range(len(duplicateset)) for dup   in duplicateset[i]));
        cur.executemany('INSERT OR IGNORE INTO min_els   VALUES(?,?)',((repID,i,) for i in range(len(min_els))    for repID in min_els[i]));
        cur.execute('CREATE INDEX labelling_label_index ON labelling(label)');
        cur.execute('CREATE INDEX min_els_label_index ON min_els(label)');
        con.commit();
        con.close();
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#SCRIPT---------------------------------------------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------------------
print('Preparing...');
#-------------------------------------------------------------------------------------------------------------------------------------
manager = MP.Manager();
Q       = manager.Queue();
queues  = [manager.Queue() for i in range(_queues)];
R       = manager.Queue();
fetcher = MP.Process(target=fetch,args=(Q,));
feeders = [MP.Process(target=feed,args=(Q,queues,x,)) for x in range(_jobs_feed)];
workers = [MP.Process(target=work,args=(queues,R,x,)) for x in range(_jobs)];
#-------------------------------------------------------------------------------------------------------------------------------------
printer('Starting fetcher...\n');
#-------------------------------------------------------------------------------------------------------------------------------------
fetcher.start();
#-------------------------------------------------------------------------------------------------------------------------------------
printer('Starting feeders...\n');
#-------------------------------------------------------------------------------------------------------------------------------------
for feeder in feeders:
    feeder.start();
    time.sleep(_wait_time);
#-------------------------------------------------------------------------------------------------------------------------------------
time.sleep(_wait);
#-------------------------------------------------------------------------------------------------------------------------------------
print('Starting workers...');
#-------------------------------------------------------------------------------------------------------------------------------------
for worker in workers:
    worker.start();
    time.sleep(_wait_time);
#-------------------------------------------------------------------------------------------------------------------------------------
print('Joining fetcher...');
#-------------------------------------------------------------------------------------------------------------------------------------
fetcher.join();
#-------------------------------------------------------------------------------------------------------------------------------------
print('Joining workers...');
#-------------------------------------------------------------------------------------------------------------------------------------
for worker in workers:
    worker.join();
#-------------------------------------------------------------------------------------------------------------------------------------
print('Joining feeder...');
#-------------------------------------------------------------------------------------------------------------------------------------
for feeder in feeders:
    feeder.join();
#-------------------------------------------------------------------------------------------------------------------------------------
print('Combining result minels...');
#-------------------------------------------------------------------------------------------------------------------------------------
if _minels:
    min_els, min_reps = [],[];
    for i in range(len(queues)):
        try:
            min_els, min_reps, min_ids = queues[i].get(timeout=0.1);
            print('Found something in queues', i);
            break;
        except:
            pass;
    min_els, min_reps, min_ids = find_min_els(min_els,min_reps,min_ids);
else:
    repIDs, reps, ids, parent, number, size2sets, el2sets = [],[],[],[],[],[],[];
    for i in range(len(queues)):
        try:
            repIDs, reps, ids, parent, number, size2sets, el2sets = queues[i].get(timeout=0.1);
            print('Found something in queues', i);
            break;
        except:
            pass;
    repIDs, reps, ids, parent, number, size2sets, el2sets = connected_components(repIDs,reps,ids,parent,number,size2sets,el2sets);
    result = make_result(repIDs,reps,ids,parent);
#-------------------------------------------------------------------------------------------------------------------------------------
if not _minels:
    write_dups(result,_outdb);
    exit();
#-------------------------------------------------------------------------------------------------------------------------------------
printer('Writing output for minimal elements...\n');
#-------------------------------------------------------------------------------------------------------------------------------------
con_out = sqlite3.connect(_outdb);
cur_out = con_out.cursor();
cur_out.execute("DROP   TABLE IF EXISTS queries");
cur_out.execute("CREATE TABLE           queries(queryID INTEGER PRIMARY KEY AUTOINCREMENT, query TEXT UNIQUE, repIDIndex INT UNIQUE, repID TEXT UNIQUE, num_spe INT, car INT, obs INT, legal BOOL)");

page = 0;
for i in range(len(min_els)): #TODO: Get the numbers...
    repID       = min_els[i];
    rep         = min_reps[i];
    repIDIndex  = i;
    obs         = None;
    car         = None;
    num_spe     = None;
    rep_        = [[('a'+str(i)+tup[0],tup[1],) for i in range(1,_max_len_+1)] for tup in rep];
    query       = "SELECT mentionID FROM publications WHERE "+' AND '.join(['('+(' OR '.join([str(el[0])+'="'+str(el[1])+'"' for el in part]))+')' for part in rep_]);
    cur_out.execute("INSERT INTO queries(query,repIDIndex,repID,num_spe,car,obs,legal) VALUES(?,?,?,?,?,?,?)",(query,repIDIndex,repID,num_spe,car,obs,True,)); con_out.commit();
    page += 1;
    if page%100==0: printer(page);

con.close(); con_out.close();
#-------------------------------------------------------------------------------------------------------------------------------------
