import sqlite3
import sys
import numpy as np
from collections import defaultdict, Counter
import time
import random
import multiprocessing as MP
import multiprocessing.pool
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components

_inDB  = sys.argv[1];
_outDB = sys.argv[2];


def get_featsOf(cur,start=None,end=None):
    print(start,end);
    if start == None:
        cur.execute("SELECT * FROM features ORDER BY repIDIndex DESC");
    else:
        cur.execute("SELECT * FROM features WHERE repIDIndex BETWEEN ? AND ? ORDER BY repIDIndex",(start,end,)); #TODO: This is quite slow! #Requires Ordering ASC by size
        #cur.execute("SELECT * FROM features ORDER BY repIDIndex DESC LIMIT ?,?",(start,end-start,)); # Needs different start,end!!
    first   = cur.fetchone();
    featsOf = [[first[1]]];
    current = first[0];
    for repIDIndex, featIndex in cur: #TODO: This could be faster but it is not as bad...
        if repIDIndex != current: # New representation
            current     = repIDIndex;
            featsOf[-1] = np.array(featsOf[-1]);
            featsOf.append([]);
            if repIDIndex % 1000000 == 0:
                print(repIDIndex);
        featsOf[-1].append(featIndex);
    featsOf[-1] = np.array(featsOf[-1]);
    featsOf = np.array(featsOf,dtype=object);
    return featsOf;

def get_structOf(cur,start,end):
    print('STRUCT: Building help structure for repIDIndices between',start,'and',end); t = time.time();
    struct = defaultdict(set);
    cur.execute("SELECT * FROM features WHERE repIDIndex BETWEEN ? AND ? ORDER BY repIDIndex",(start,end,)); #TODO: This is quite slow! #Requires Ordering ASC by size
    for repIDIndex, featIndex in cur:
        struct[featIndex].add(repIDIndex);
    print('STRUCT: Took',time.time()-t,'seconds.');
    return struct;#, time.time()-t;

def make_setsOf(C):
    setsOf   = dict();
    maxfeat  = 0;
    for i in range(len(C)):
        if i % 1000000 == 0:
            print(i);
        for feat in C[i]:
            if feat in setsOf:
                setsOf[feat].append(i);
            else:
                setsOf[feat] = [i];
                if maxfeat < feat:
                    maxfeat = feat;
    return np.array([np.array(setsOf[feat]) if feat in setsOf else np.array([]) for feat in range(maxfeat+1)], dtype=object);

def sort_reps(featsOf,setsOf):
    featSize  = np.array([len(a) for a in setsOf]);
    featOrder = np.argsort(np.argsort(featSize));
    for i in range(len(featsOf)):
        if i % 1000000 == 0:
            print(i);
        if len(featsOf[i]) > 0:
            featsOf[i] = featsOf[i][np.argsort(featOrder[featsOf[i]])];
    return featsOf;

def pad_to_dense(M,maxlen=None): #TODO: Make in-place
    maxlen = max(len(r) for r in M) if maxlen==None else maxlen;
    Z      = -np.ones((len(M), maxlen));
    for enu, row in enumerate(M):
        if len(row) > 0:
            Z[enu,-len(row):] += row+1;
    return Z;

def get_unique(D):
    D_                          = pad_to_dense(D);
    _, unique, mention2repIndex = np.unique(D_,axis=0,return_index=True,return_inverse=True); #TODO: Could be modified to obtain lexicographic order on features regarding frequency of occurance
    return D[unique], mention2repIndex;

def get_sets(featsOf):
    featsOf_ = np.array([np.array(list(set(featsOf[i]))) for i in range(len(featsOf))],dtype=object);
    return featsOf_;

def put(value,queue,sleeptime=0.1,max_trytime=1):
    start_time = time.time();
    try_time   = 0;
    while True:
        try:
            queue.put(value,block=False);
            #print('...done put...');
            break;
        except Exception as e:
            try_time = time.time() - start_time;
            if try_time > max_trytime:
                #print('Failed to put.');
                return 1;
            time.sleep(sleeptime);

def get(queue,sleeptime=0.02,max_trytime=0.1):
    start_time = time.time();
    try_time   = 0;
    value      = None;
    while True:
        try:
            value = queue.get(block=False);
            #print('...done get...');
            break;
        except Exception as e:
            try_time = time.time() - start_time;
            if try_time > max_trytime:
                #print('Failed to get.');
                break;
            time.sleep(sleeptime);
    return value;

def make_struct(cur,start,end,size): #Assumes that C_global is cardinality-sorted
    print('STRUCT: Building help structure for search size',size); t = time.time();
    C      = get_featsOf(cur,start,end); print('Got rows for help structure.')
    struct = defaultdict(set);
    for index in range(len(C)): #TODO: This also takes quite long!
        size_ = len(C[index]);
        if size_ == size:
            for feat in C[index]:
                struct[feat].add(start+index);
        elif size_ > size:
            pass;#break;
    print('STRUCT: Took',time.time()-t,'seconds.'); #TODO: At this point we have the current struct, C, and the next struct all in memory!
    return struct;

def find_subsup(start,end,search_size):
    #print('SUBSUP: Getting subsets and supersets for search size '+str(search_size)+' from '+str(start)+' to '+str(end)+'...'); t = time.time();
    con       = sqlite3.connect(_inDB);
    cur       = con.cursor();
    C         = get_featsOf(cur,start,end); con.close(); #TODO: Because of the appending, this assumes that there are no gaps in the repIDIndex!
    subsets   = [];                                      #TODO: However it seems that there are indeed no gaps.
    supersets = [];
    for index1 in range(start,end+1):
        if len(C[index1-start]) >= search_size:
            break;
        feats      = [feat for freq,feat in sorted([(len(S[feat]),feat,) for feat in C[index1-start]])];
        overlaps   = (S[feat] if feat in S else set() for feat in feats);
        supersets_ = set.intersection(*[overlap for overlap in overlaps]); #Could write this explicitely going through the feat-sets and breaking when having found as many supersets as frequency of first in struct
        if len(supersets_) > 0:
            subsets.append(index1);
            supersets.append(supersets_)
        #if index1 % 500000 == 0:
        #    print(len(feats),index1,round(time.time()-t,2));
    print('SUBSUP: Found',len(subsets),' subsets from '+str(start)+' to '+str(end)+' for superset-size '+str(search_size)+' in ',round(time.time()-t),'s');
    return subsets, supersets;

def work(Q,R):
    #print('Length of struct:',len(S));
    while True:
        job = get(Q);
        if job != None:
            start1, end1, size = job;
            subsets, supersets = find_subsup(start1,end1,size);
            put((subsets,supersets,),R);
        else:
            #print('Closing worker...');
            break;
    #return 0;

def size_boundaries(cur):
    cur.execute("SELECT freq, COUNT(*) FROM (SELECT COUNT(*) AS freq FROM features GROUP BY repIDIndex) GROUP BY freq");
    size_freq  = {size:freq for size,freq in cur};
    sizes      = sorted(list(size_freq.keys()));
    boundaries = [[None,None,] for i in range(max(sizes)+1)];
    acc = 0;
    for size in sizes:
        boundaries[size][0] = acc;
        acc                += size_freq[size];
        boundaries[size][1] = acc-1;
    return sizes,boundaries;

def queue2list(Q):
    L = [];
    while True:
        element = get(Q);
        if element == None:
            break;
        L.append(element);
    return L;

def get_min_el_map_(cur,sizes,boundaries,num_workers=16,batch_factor=2,patchsize=1000000): #Assuming that sizes is smallest first! #TODO: Check start and end!
    global S;
    timings    = [];
    min_el_map = defaultdict(set);
    supersets  = set();
    manager    = MP.Manager();
    for k in range(1,len(sizes)): #we iterate over the superset sizes starting at the second size and get to the subset sizes by minus 1
        t         = time.time();
        prev_size = sizes[k-1];
        this_size = sizes[k];
        next_size = sizes[k+1] if k+1 < len(sizes) else None;
        size_end  = boundaries[prev_size][1];# if next_size != None else boundaries[this_size][0];
        start,end = boundaries[this_size][0],boundaries[this_size][1];
        patches   = [(i,min(i+patchsize,end)) for i in range(start,end,patchsize)]; print('We have',len(patches),'patches.'); #patches for second size
        batchsize = 1+int((size_end)/(batch_factor*num_workers)); #Make more batches than workers to better distribute workload despite different prob sizes
        batches   = [(i,min(i+batchsize,size_end)) for i in range(0,size_end,batchsize)]; print('We have',len(batches),'batches.');
        timings.append(('size_preparation',time.time()-t,));
        S,took    = get_structOf(cur,patches[0][0],patches[0][1]); #TODO: For now we have a wait after each size, change to size,patch tupels...
        timings.append(('stuct_create',took,));
        for l in range(len(patches)):
            t                = time.time();
            fro,to           = patches[l];
            print('##### FINDING SIZE-'+str(sizes[k])+'-SUPERSETS FOR POTENTIAL SUBSETS WITH SIZES UP TO '+str(sizes[k-1])+', patch',fro,'-',to,'...');
            fro_next,to_next = patches[l+1] if l+1<len(patches) else (None,None);
            Q                = manager.Queue();
            R                = manager.Queue();
            workers          = [MP.Process(target=work,args=(Q,R,)) for x in range(num_workers)];
            for batch_start,batch_end in batches:
                put((batch_start,batch_end,this_size,),Q);
            timings.append(('patch_preparation',time.time()-t,)); t=time.time();
            for worker in workers:
                worker.start();
            timings.append(('workers_start',time.time()-t,)); t=time.time();
            print('Started all workers. Building next struct...');
            S_next,took = get_structOf(cur,fro_next,to_next) if fro_next != None else (None,0.0);
            timings.append(('stuct_create',took,));
            print('Done building next struct. Took',time.time()-t,'s'); t=time.time();
            to_join = set(range(len(workers)));
            while len(to_join) > 0:
                i = random.sample(to_join,1)[0];
                workers[i].join(0.1);
                if not workers[i].is_alive():
                    to_join.remove(i); print(len(to_join),'workers left to join.');
                else:
                    time.sleep(0.2);
            timings.append(('worker_join',time.time()-t,)); t = time.time(); print('Joined all workers. Getting results from queue...');
            results              = queue2list(R);
            timings.append(('results_get',time.time()-t,)); t = time.time(); print('Unpacking results...');
            subsetss, supersetss = zip(*results);
            current_subsets      = set(         [subset   for subsets   in subsetss   for subset   in subsets]);
            current_supersets    = set().union(*[superset for supersets in supersetss for superset in supersets]);
            timings.append(('results_unpack',time.time()-t,)); t = time.time(); print('Updating supersets...');
            supersets           |= current_supersets;
            timings.append(('superset_update',time.time()-t,)); t = time.time(); print('Getting current minimum elements...');
            current_min_els      = current_subsets - supersets;
            timings.append(('minel_get',time.time()-t,)); t = time.time(); print('Updating minel map...');
            for subsets_, supersets_ in results:
                for i in range(len(subsets_)):
                    if subsets_[i] in current_min_els:
                        min_el_map[subsets_[i]] |= supersets_[i];
            timings.append(('minel_update',time.time()-t,)); t = time.time(); print('Overwriting search index...');
            del S;
            S = S_next;
            timings.append(('struct_overwrite',time.time()-t,)); t = time.time(); print('Next iteration');
        print('#### TIMINGS:',timings);
    return min_el_map, supersets, timings;

def join(workers):
    to_join = set(range(len(workers)));
    while len(to_join) > 0:
        i = random.sample(to_join,1)[0];
        workers[i].join(0.1);
        if not workers[i].is_alive():
            to_join.remove(i); print(len(to_join),'workers left to join.',end='\r');
        else:
            time.sleep(0.2);

def start(workers,batches,Q,size):
    for batch_start,batch_end in batches:
        put((batch_start,batch_end,size,),Q);
    for worker in workers:
        worker.start();

def update_results(supersets,min_el_map,R):
    results              = queue2list(R);
    subsetss, supersetss = zip(*results);
    current_subsets      = set(         [subset   for subsets   in subsetss   for subset   in subsets]);
    current_supersets    = set().union(*[superset for supersets in supersetss for superset in supersets]);
    supersets           |= current_supersets;
    current_min_els      = current_subsets - supersets;
    for subsets_, supersets_ in results:
        for i in range(len(subsets_)):
            if subsets_[i] in current_min_els:
                min_el_map[subsets_[i]] |= supersets_[i];
    return supersets, min_el_map;

def make_tasks(batchsize,patchsize,sizes,boundaries):
    tasks = [];
    for k in range(1,len(sizes)):
        #---------------------------------------------------------------------------------------------------------------------------
        sub_start, sub_end = boundaries[sizes[0]][0], boundaries[sizes[k-1]][1];
        sup_start, sup_end = boundaries[sizes[k]][0], boundaries[sizes[k  ]][1];
        #---------------------------------------------------------------------------------------------------------------------------
        batches = [(i,min(i+batchsize,sub_end)) for i in range(sub_start,sub_end,batchsize)];
        patches = [(i,min(i+patchsize,sup_end)) for i in range(sup_start,sup_end,patchsize)];
        #---------------------------------------------------------------------------------------------------------------------------
        tasks += [(patch,batches,sizes[k],) for patch in patches];
    return tasks;

def get_min_el_map(cur,sizes,boundaries,num_workers=16,batchsize=1000000,patchsize=1000000): #Assuming that sizes is smallest first!
    #---------------------------------------------------------------------------------------------------------------------------
    global S, _cur_out;
    #---------------------------------------------------------------------------------------------------------------------------
    min_el_map, supersets, manager = defaultdict(set), set(), MP.Manager();
    #---------------------------------------------------------------------------------------------------------------------------
    tasks = make_tasks(batchsize,patchsize,sizes,boundaries);
    #---------------------------------------------------------------------------------------------------------------------------
    patch, batches, size = tasks[0];
    S                    = get_structOf(cur,patch[0],patch[1])
    for i in range(1,len(tasks)): # sup_size, patch_start, patch_end, t1, t2, t3, t4, t5
        #-1-------------------------------------------------------------------------------------------------------------------------
        Q, R    = manager.Queue(), manager.Queue();
        workers = [MP.Process(target=work,args=(Q,R,)) for x in range(num_workers)];
        #-2-------------------------------------------------------------------------------------------------------------------------
        start(workers,batches,Q,size);
        #-3-------------------------------------------------------------------------------------------------------------------------
        patch, batches, size = tasks[i];
        S                    = get_structOf(cur,patch[0],patch[1]);
        #-4-------------------------------------------------------------------------------------------------------------------------
        join(workers);
        #-5-------------------------------------------------------------------------------------------------------------------------
        supersets, min_el_map = update_results(supersets,min_el_map,R);
        #-6-------------------------------------------------------------------------------------------------------------------------
    del S;
    return min_el_map, supersets;

#------------------------------------------------------------
con     = sqlite3.connect(_inDB);
cur     = con.cursor();
con_out = sqlite3.connect(_outDB);
cur_out = con_out.cursor();
#------------------------------------------------------------
sizes, boundaries              = size_boundaries(cur);
input('Enter to continue...');
min_el_map, supersets = get_min_el_map(cur,sizes,boundaries,1,1000000,1000000);
#------------------------------------------------------------
rows, cols    = zip(*[(fro,to,) for fro in min_el_map for to in min_el_map[fro]]);
max_index     = boundaries[-1][1];#max(max(min_el_map.keys()),max(supersets));
min_el_matrix = csr((np.ones(len(rows),dtype=bool),(rows,cols,)), dtype=bool, shape=(max_index+1,max_index+1,) );
num_c, labels = connected_components(min_el_matrix,directed=False);
#------------------------------------------------------------
components = [set() for i in range(num_c)];
for repIndex in range(len(labels)):
    components[labels[repIndex]].add(repIndex);
components = [component for component in components if len(component) > 1];
#------------------------------------------------------------
OUT = open('components_test_disk.txt','w');
for i in range(len(components)):
    if len(components[i]) < 1000: # This is only a hack. We should never end up with such large components
        OUT.write(' '.join([str(repIndex) for repIndex in components[i]])+'\n');
OUT.close();
#------------------------------------------------------------
con_out = sqlite3.connect('components_large_disk.db');
cur_out = con_out.cursor();
cur_out.execute("DROP TABLE IF EXISTS components");
cur_out.execute("CREATE TABLE components(label INT, repIDIndex INT)");
cur_out.executemany("INSERT INTO components VALUES(?,?)",((i,repIndex,) for i in range(len(components)) if len(components[i]) < 25000 for repIndex in components[i]));
con_out.commit();
cur_out.execute("DROP   INDEX IF EXISTS label_index");
cur_out.execute("CREATE INDEX label_index on components(label)");
con_out.close();
#------------------------------------------------------------
cfreq = np.array([(i,len(components[i])) for i in range(len(components))]);
csort = np.argsort(cfreq[:,1]);
mfreq = np.array([(min_el,len(min_el_map[min_el])) for min_el in min_el_map]);
msort = np.argsort(mfreq[:,1]);
#------------------------------------------------------------
min_els     = set([min_el for min_el in min_el_map if len(min_el_map[min_el])>0]);
min_reduce  = [(cfreq[csort[i]][1],set([mention2repIndex[mentionIndex] for mentionIndex in components[csort[i]]])&min_els,csort[i]) for i in range(len(csort))];
min_reduce_ = [(f1,len(f2),f3,) for f1,f2,f3 in min_reduce if len(f2)>0];
min_reduced = [(f1,f2,f3,) for f1,f2,f3 in min_reduce if len(f2)>0];
#------------------------------------------------------------
OUT = open('component_minels_disk.txt','w');
for compsize,minels,compindex in min_reduced:
    OUT.write(' '.join([str(next(iter(rep2mentionIndex[repIndex]))) for repIndex in minels])+'\n');
OUT.close();
#------------------------------------------------------------
