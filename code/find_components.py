import sqlite3
import sys
import numpy as np
from collections import defaultdict, Counter
import time
import multiprocessing as MP
import multiprocessing.pool
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components

_inDB  = sys.argv[1];


def get_featsOf(cur):
    cur.execute("SELECT * FROM features ORDER BY mentionIDIndex");
    featsOf = [[]];
    prev    = 0;
    for mentionIDIndex, featIndex in cur:
        while mentionIDIndex != prev:
            prev       += 1;
            if prev != mentionIDIndex:
                print('Empty representation with mentionIDIndex',mentionIDIndex);
            featsOf[-1] = np.array(featsOf[-1]);
            featsOf.append([]);
            if mentionIDIndex % 1000000 == 0:
                print(mentionIDIndex);
        featsOf[mentionIDIndex].append(featIndex);
    featsOf = np.array(featsOf,dtype=object);
    return featsOf;

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
            print('...done put...');
            break;
        except Exception as e:
            try_time = time.time() - start_time;
            if try_time > max_trytime:
                print('Failed to put.');
                return 1;
            time.sleep(sleeptime);

def get(queue,sleeptime=0.02,max_trytime=0.1):
    start_time = time.time();
    try_time   = 0;
    value      = None;
    while True:
        try:
            value = queue.get(block=False);
            print('...done get...');
            break;
        except Exception as e:
            try_time = time.time() - start_time;
            if try_time > max_trytime:
                print('Failed to get.');
                break;
            time.sleep(sleeptime);
    return value;

def make_struct(start,end,size): #Assumes that C_global is cardinality-sorted
    print('STRUCT: Building help structure for search size',size); t = time.time();
    struct = defaultdict(set);
    for index in range(start,end+1):
        size_ = len(C_global[index]);
        if size_ == size:
            for feat in C_global[index]:
                struct[feat].add(index);
        elif size_ > size:
            break;
    print('STRUCT: Took',time.time()-t,'seconds.');
    return struct;

def find_subsup(start,end,size): #Assume all indices in indices2 point to itemsets of the same size!
    print('SUBSUP: Getting subsets and supersets for search size '+str(size)+' from '+str(start)+' to '+str(end)+'...'); t = time.time();
    subsets   = [];
    supersets = [];
    for index1 in range(start,end+1):
        if len(C_global[index1]) >= size:
            break;
        if not C_global[index1][0] in S: #Use least frequent element for early stop, might apply quite frequently
            continue;
        feats      = [feat for freq,feat in sorted([(len(F_global[feat]),feat,) for feat in C_global[index1]])];
        overlaps   = (S[feat] if feat in S else set() for feat in feats);
        supersets_ = set.intersection(*[overlap for overlap in overlaps]); #Could write this explicitely going through the feat-sets and breaking when having found as many supersets as frequency of first in struct
        if len(supersets_) > 0:
            subsets.append(index1);
            supersets.append(supersets_); #TODO: This is not acceptable as it completely reproduces the partial order!
        if index1 % 500000 == 0:
            print(len(feats),index1,round(time.time()-t,2));
    print('SUBSUP: Took',time.time()-t,'seconds.');
    return subsets, supersets;

def work(Q,R):
    print('Length of struct:',len(S));
    while True:
        job = get(Q);
        if job != None:
            start1, end1, size = job;
            subsets, supersets = find_subsup(start1,end1,size);
            put((subsets,supersets,),R);
        else:
            print('Closing worker...');
            break;
    return 0;

def size_boundaries(start,end,sizes):
    boundaries = [[0,0,] for i in range(max(sizes)+1)];
    prev_size  = 0;
    size       = 0;
    for index in range(start,end):
        prev_size = size;
        size      = len(C_global[index]);
        if prev_size < size:
            print(prev_size,size);
            boundaries[size][0] = index;
        boundaries[size][1] = index;
    boundaries[size][1] += 1;
    return boundaries;

def parallel_sub_sups(all_indices,sizes,boundaries,num_workers=16,batch_factor=2): #Assuming that sizes is inverted, largest first!
    global S;
    manager = MP.Manager();
    Q       = manager.Queue();
    R       = manager.Queue();
    start   = all_indices[0];
    S       = make_struct(boundaries[sizes[0]][0],boundaries[sizes[0]][1],sizes[0]);
    for k in range(len(sizes)):
        print(sizes[k],': FINDING SUPERSETS...');
        this_size    = sizes[k];
        next_size    = sizes[k+1] if k+1 < len(sizes) else None;
        end          = boundaries[next_size][1] if next_size != None else boundaries[this_size][0];
        batchsize    = 1+int((end-start)/(batch_factor*num_workers)); #Make more batches than workers to better distribute workload despite different prob sizes
        batches      = [(i,min(i+batchsize,end)) for i in range(start,end,batchsize)]; print('We have',len(batches),'batches.');
        workers      = [MP.Process(target=work,args=(Q,R,)) for x in range(num_workers)];
        for batch_start,batch_end in batches:
            put((batch_start,batch_end,this_size,),Q);
        t = time.time();
        for worker in workers:
            worker.start();
            print('Forking took', time.time()-t, 'seconds.'); t=time.time();
        S_next = make_struct(boundaries[next_size][0],boundaries[next_size][1],next_size) if next_size != None else None;
        for worker in workers:
            worker.join();
        S = S_next;
    results = [];
    while True:
        result = get(R);
        if result == None:
            break;
        results.append(result);
    subsetss, supersetss = zip(*results);
    subsets              = set(         [subset   for subsets   in subsetss   for subset   in subsets]);
    supersets            = set().union(*[superset for supersets in supersetss for superset in supersets]);
    only_subsets         = subsets - supersets;
    min_el_map           = defaultdict(set);
    for subsets_, supersets_ in results:
        for i in range(len(subsets_)):
            if subsets_[i] in only_subsets:
                min_el_map[subsets_[i]] |= supersets_[i];
    return subsets, supersets, min_el_map;

def queue2list(Q):
    L = [];
    while True:
        element = get(Q);
        if element == None:
            break;
        L.append(element);
    return L;

def get_min_el_map(all_indices,sizes,boundaries,num_workers=16,batch_factor=2): #Assuming that sizes are smallest first!
    global S;
    min_el_map = defaultdict(set);
    supersets  = set();
    S          = make_struct(boundaries[sizes[0]][0],boundaries[sizes[0]][1],sizes[0]);
    for k in range(len(sizes)):
        print(sizes[k],': FINDING SUPERSETS...');
        this_size    = sizes[k];
        next_size    = sizes[k+1] if k+1 < len(sizes) else None;
        end          = boundaries[next_size][1] if next_size != None else boundaries[this_size][0];
        batchsize    = 1+int((end)/(batch_factor*num_workers)); #Make more batches than workers to better distribute workload despite different prob sizes
        batches      = [(i,min(i+batchsize,end)) for i in range(0,end,batchsize)]; print('We have',len(batches),'batches.');
        manager      = MP.Manager();
        Q            = manager.Queue();
        R            = manager.Queue();
        workers      = [MP.Process(target=work,args=(Q,R,)) for x in range(num_workers)];
        for batch_start,batch_end in batches:
            put((batch_start,batch_end,this_size,),Q);
        t = time.time();
        for worker in workers:
            worker.start();
            print('Forking took', time.time()-t, 'seconds.'); t=time.time();
        S_next = make_struct(boundaries[next_size][0],boundaries[next_size][1],next_size) if next_size != None else None;
        for worker in workers:
            worker.join();
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
        S = S_next;
    return min_el_map;

#------------------------------------------------------------
con                 = sqlite3.connect(_inDB);
cur                 = con.cursor();                                print('Loading featsOf...');
featsOf             = get_featsOf(cur);                            print('Replacing featsOf multisets by sets...');
featsOf             = get_sets(featsOf);                           print('Getting unique...');
C, mention2repIndex = get_unique(featsOf);                         print('Deleting featsOf...');
del featsOf;                                                       print('Making inverted feat to sets index...');
setsOf              = make_setsOf(C);                              print('Sorting individual itemsets by item frequency...');
C                   = sort_reps(C,setsOf);                         #This might also go before make_unique as it then affects the itemset order within same cardinality but is more expensive
con.close();
#------------------------------------------------------------
#input('Enter to continue...');
#------------------------------------------------------------
C_global = C;
F_global = setsOf;
#------------------------------------------------------------
all_indices = np.arange(1,len(C),1);
sizes       = [size for size in sorted(list(set([len(C_global[index]) for index in all_indices])),reverse=True) if size>1];
boundaries  = size_boundaries(all_indices[0],all_indices[-1],sizes);
min_el_map  = get_min_el_map(all_indices,sizes[::-1],boundaries,8,4);
#subsets, supersets, min_el_map = parallel_sub_sups(all_indices,sizes,boundaries,8,4);
#------------------------------------------------------------
rows, cols    = zip(*[(fro,to,) for fro in min_el_map for to in min_el_map[fro]]); max_index = max(all_indices);
min_el_matrix = csr((np.ones(len(rows),dtype=bool),(rows,cols,)), dtype=bool, shape=(max_index+1,max_index+1,) );
num_c, labels = connected_components(min_el_matrix,directed=False);
#------------------------------------------------------------
rep2mentionIndex = defaultdict(set);
for mentionIndex in range(len(mention2repIndex)):
    rep2mentionIndex[mention2repIndex[mentionIndex]].add(mentionIndex);
#------------------------------------------------------------
components = [set() for i in range(num_c)];
for repIndex in range(len(labels)):
    for mentionIndex in rep2mentionIndex[repIndex]:
        components[labels[repIndex]].add(mentionIndex);
components = [components[i] for i in range(len(components)) if len(components[i]) > 1];
#------------------------------------------------------------
OUT = open('components_test.txt','w');
for i in range(len(components)):
    if len(components[i]) < 1000: # This is only a hack. We should never end up with such large components
        OUT.write(' '.join([str(mentionIndex) for mentionIndex in components[i]])+'\n');
OUT.close();
#------------------------------------------------------------
con_out = sqlite3.connect('components_large.db');
cur_out = con_out.cursor();
cur_out.execute("DROP TABLE IF EXISTS components");
cur_out.execute("CREATE TABLE components(label INT, mentionIDIndex INT)");
cur_out.executemany("INSERT INTO components VALUES(?,?)",((i,mentionIndex,) for i in range(len(components)) if len(components[i]) < 25000 for mentionIndex in components[i]));
con_out.commit();
cur_out.execute("DROP   INDEX IF EXISTS label_index");
cur_out.execute("CREATE INDEX label_index on components(label)");
con_out.close();
#------------------------------------------------------------
ffreq = np.array([(i,len(F_global[i])) for i in range(len(F_global))]);
fsort = np.argsort(ffreq[:,1]);
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
OUT = open('component_minels.txt','w');
for compsize,minels,compindex in min_reduced:
    OUT.write(' '.join([str(next(iter(rep2mentionIndex[repIndex]))) for repIndex in minels])+'\n');
OUT.close();
#------------------------------------------------------------
