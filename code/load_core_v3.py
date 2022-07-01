import sqlite3
import sys
import numpy as np
from collections import defaultdict
import time
import multiprocessing as MP
import multiprocessing.pool
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components

_inDB  = sys.argv[1];


class NoDaemonProcess(MP.Process):
    @property
    def daemon(self):
        return False;
    @daemon.setter
    def daemon(self, value):
        pass;

class NoDaemonContext(type(MP.get_context())):
    Process = NoDaemonProcess;

class NestablePool(MP.pool.Pool):
    def __init__(self, *args, **kwargs):
        kwargs['context'] = NoDaemonContext();
        super(NestablePool, self).__init__(*args, **kwargs);


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

def get_setsOf(cur):
    cur.execute("SELECT * FROM features ORDER BY featIndex");
    setsOf = [[]];
    prev   = 0;
    for mentionIDIndex, featIndex in cur:
        while featIndex != prev:
            prev       += 1;
            if prev != featIndex:
                print('Empty representation with featIndex',featIndex);
            setsOf[-1] = np.array(setsOf[-1]);
            setsOf.append([]);
            if featIndex % 1000000 == 0:
                print(featIndex);
        setsOf[featIndex].append(mentionIDIndex);
    setsOf = np.array(setsOf,dtype=object);
    return setsOf;

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

def remove_singleFeats(C,setsOf):
    repsOf = defaultdict(set);
    for repIndex in range(len(C)):
        for feat in C[repIndex]:
            repsOf[feat].add(repIndex);
    featSize_   = np.array([len(repsOf[i]) for i in range(len(setsOf))]);
    singfeats   = (featSize_==1).nonzero()[0];
    singfeatset = set(singfeats);
    C_mod       = np.array([np.array([feat for feat in rep if not feat in singfeatset]) for rep in C],dtype=object);

def get_cardinality_sorting(featsOf):
    buckets = [[] for i in range(100)];
    for i in range(len(featsOf)):
        buckets[len(featsOf[i])].append(i);
    sorting = [index for bucket in buckets for index in bucket];
    del buckets;
    return sorting;

def sort_reps(featsOf,setsOf):
    featSize  = np.array([len(a) for a in setsOf]);
    featOrder = np.argsort(np.argsort(featSize));
    for i in range(len(featsOf)):
        if i % 1000000 == 0:
            print(i);
        if len(featsOf[i]) > 0:
            featsOf[i] = featsOf[i][np.argsort(featOrder[featsOf[i]])];
    return featsOf;

#def get_mention2repIndex(unique_,len_C):
#    unique = sorted(unique_);
#    return np.array([i for i in range(len(unique)-1) for mentionIndex in range(unique[i],unique[i+1])] + [len(unique)-1 for mentionIndex in range(unique[-1],len_C)]);

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

def make_sets(featsOf):
    for i in range(len(featsOf)):
        featsOf[i] = np.array(set(featsOf[i]));

def get_sets(featsOf):
    featsOf_ = np.array([np.array(list(set(featsOf[i]))) for i in range(len(featsOf))],dtype=object);
    return featsOf_;

def compress(indices):
    z = [(a[i],a[i+1]) for i in range(len(a)-1) if a[i+1]-a[i] > 1];
    return [range(a[0],z[0][0]+1)] + [range(z[i][1],z[i+1][0]+1) for i in range(len(z)-1)] + [range(z[-1][1],a[-1]+1)];

def split_compressed(indices):
    lengths = [r.__reduce__()[1][1]-r.__reduce__()[1][0] for r in indices];
    half    = int((sum(lengths)+1)/2);
    acc, R  = 0, None;
    for i in range(len(lengths)):
        if acc+lengths[i] > half:
            R = i;
            break;
        acc += lengths[i];
    left     = half-acc;
    indices1 = indices[:R] + [range(indices[R].__reduce__()[1][0],indices[R].__reduce__()[1][0]+left)] if left != 0 else indices[:R];
    indices2 = [range(indices[R].__reduce__()[1][0]+left,indices[R].__reduce__()[1][1])] + indices[R+1:] if len(indices)>R+1 else [range(indices[R].__reduce__()[1][0]+left,indices[R].__reduce__()[1][1])];
    return indices1, indices2;

def is_subset(a,b): #a is subset of b, so all elements of a must be in b
    if len(a) >= len(b):
        return False;
    b_ = set(b);
    for el in a:
        if not el in b_:
            return False;
    return True;

def unite(indices1,indices2):
    return np.array(sorted(list(set(indices1)|set(indices2))));

def split(indices,stratified=False):
    if stratified:
        return indices[list(range(0,len(indices),2))], indices[list(range(1,len(indices),2))];
    return indices[:int(len(indices)/2)], indices[int(len(indices)/2):];

def get_max_els(indices): # assuming that itemsets in D are sorted by cardinality and items in itemsets by occurance frequency
    subsets = find_subsets(indices);
    max_els = np.array(sorted([index for index in indices if not index in subsets]));
    return max_els;

def find_subsets(indices): # assuming that itemsets in D are sorted by cardinality and items in itemsets by occurance frequency
    #shm_ref  = shared_memory.SharedMemory(name=shm.name);
    #C_local  = np.ndarray(C_global.shape, dtype=C_global.dtype, buffer=shm_ref.buf);
    D        = C_global[indices];
    #-----------------------------------------------------------------------------
    subsets = set([]);
    B       = set([]);           # 
    c       = 0;                 # current length of itemset
    O       = defaultdict(list); # all itemset indices with a certain item
    #-----------------------------------------------------------------------------
    t = time.time();
    for i in range(len(D)):
        if len(D[i]) == 0:
            continue;
        if i % 100000 == 0:
            print(c,i,len(B),time.time()-t);
            t = time.time();
        for j in range(len(D[i])):              #for all items in itemset D[i]
            for k in O[D[i][j]]:                #for all reference itemset_indices that have been added into O for the current item
                if not k in subsets:            #if current reference itemset has no known supersets
                    S = D[k];                   #get reference itemset from reference itemset_index
                    if len(S) > len(D[i])-j+1:  #if the length of the current reference itemset is larger than the length of the itemset -j+1
                        break;
                    if is_subset(S,D[i]):       #if itemset is superset of reference itemset
                        subsets.add(k);         #add itemset index to supersets of reference itemset
        if len(D[i]) > c:
            for k in B:
                S = D[k];
                O[S[0]].append(k);
            B = set([]);
            c = len(D[i]);
        B.add(i);
    #-----------------------------------------------------------------------------
    return set([indices[subset] for subset in subsets]);

def find_supersets(indices1,indices2): #Look for all supersets of indices1 in indeces2
    supersets = [];
    sizes     = sorted(list(set([len(C_global[index2]) for index2 in indices2])));
    for size in sizes:
        if size == 1: #Assuming we do not have the empty set in indices1
            continue;
        #-----------------------------------------------------------------------------
        print('Building help structure for size',size); t = time.time();
        struct = defaultdict(set);
        for index2 in indices2:
            size_ = len(C_global[index2]);
            if size_ == size:
                for feat in C_global[index2]:
                    struct[feat].add(index2);
            elif size_ > size:
                break;
        print('Took',time.time()-t,'seconds.'); i=0; t=time.time(); 
        #-----------------------------------------------------------------------------
        for index1 in indices1:
            #--------------------------------------------------------------
            i += 1;
            if i % 500000 == 0:
                print(len(feats),i,round(time.time()-t,2));
            #--------------------------------------------------------------
            if len(C_global[index1]) >= size:
                break;
            if not C_global[index1][0] in struct: #Use least frequent element for early stop, might apply quite frequently
                continue;
            feats      = [feat for freq,feat in sorted([(len(F_global[feat]),feat,) for feat in C_global[index1]])];
            overlaps   = (struct[feat] if feat in struct else set() for feat in feats);
            supersets_ = set.intersection(*[overlap for overlap in overlaps]); #Could write this explicitely going through the feat-sets and breaking when having found as many supersets as frequency of first in struct
            supersets.append((index1,supersets_,));
    return supersets;


def merge_max_els(indices1,indices2):
    #shm_ref   = shared_memory.SharedMemory(name=shm.name);
    #shm2_ref  = shared_memory.SharedMemory(name=shm2.name);
    #C_local   = np.ndarray(C_global.shape, dtype=C_global.dtype, buffer=shm_ref.buf);
    #F_local   = np.ndarray(F_global.shape, dtype=F_global.dtype, buffer=shm2_ref.buf);
    indices1_ = set(indices1);
    indices2_ = set(indices2);
    subsets1  = set();
    subsets2  = set();
    #-----------------------------------------------------------------------------
    print('Looking for supersets of collection 1 in collection 2'); t = time.time();
    for index1 in indices1: #looking for supersets of itemsets1 in itemsets2
        first = C_global[index1][0];
        size  = len(C_global[index1]);
        for index2 in (index for index in F_global[first][::-1] if index in indices2_): #going from largest itemsets first - C[index1] should be cardinality sorted
            if len(C_global[index2]) <= size:
                break;
            if is_subset(C_global[index1],C_global[index2]):
                subsets1.add(index1);
                break;
    #-----------------------------------------------------------------------------
    print('Took',time.time()-t,'seconds.');
    print('Looking for supersets of collection 2 in collection 1'); t = time.time();
    for index2 in indices2: #looking for supersets of itemsets2 in itemsets1
        first = C_global[index2][0];
        size  = len(C_global[index2]);
        for index1 in (index for index in F_global[first][::-1] if index in indices1_): #going from largest itemsets first - C[index1] should be cardinality sorted
            if len(C_global[index1]) <= size:
                break;
            if is_subset(C_global[index2],C_global[index1]):
                subsets2.add(index2);
                break;
    #-----------------------------------------------------------------------------
    print('Found',len(subsets2),'subsets in max_els2.'); print('Took',time.time()-t,'seconds.');
    print('Combining max_el information'); t = time.time();
    max_els1 = [max_el for max_el in indices1 if not max_el in subsets1];
    max_els2 = [max_el for max_el in indices2 if not max_el in subsets2];
    print('Took',time.time()-t,'seconds.');
    #-----------------------------------------------------------------------------
    return max_els1, max_els2;

#def parallel_max_merge(indices1,indices2,returns): #TODO: Somehow too much overhead. The larger the batch, the larger the overhead...
def parallel_max_merge(indices1,indices2):
    ##manager = MP.Manager();
    ##results = manager.Queue();
    max_els_1, max_els_2 = [None,None];
    if len(indices1) <= _batch_size_merge and len(indices2) <= _batch_size_merge:
        max_els_1, max_els_2 = merge_max_els(indices1,indices2);
    elif len(indices1) > _batch_size_merge and len(indices2) > _batch_size_merge:
        indices1a, indices1b = split(indices1,True);
        indices2a, indices2b = split(indices2,True);
        with NestablePool(processes=2) as pool:
            [indices1a,indices2a], [indices1b,indices2b] = pool.starmap(parallel_max_merge,[(indices1a,indices2a,),(indices1b,indices2b,)]);
            [indices1a,indices2b], [indices1b,indices2a] = pool.starmap(parallel_max_merge,[(indices1a,indices2b,),(indices1b,indices2a,)]);
        #-----------------------------------------------------------------------------------
        ##p1      = MP.Process(target=parallel_max_merge,args=(indices1a,indices2a,results,));
        ##p2      = MP.Process(target=parallel_max_merge,args=(indices1b,indices2b,results,));
        ##p1.start(); p2.start();
        ##p1.join();  p2.join();
        ##indices1a, indices2a = results.get(); # Note that you do not know if these are the original 1a,2a or rather 1b,2b
        ##indices1b, indices2b = results.get();
        #-----------------------------------------------------------------------------------
        ##p1      = MP.Process(target=parallel_max_merge,args=(indices1a,indices2b,results,));
        ##p2      = MP.Process(target=parallel_max_merge,args=(indices1b,indices2a,results,));
        ##p1.start(); p2.start();
        ##p1.join();  p2.join();
        ##indices1a, indices2b = results.get(); # Note that you do not know if these are the original 1a,2b or rather 1b,2a
        ##indices1b, indices2a = results.get();
        #-----------------------------------------------------------------------------------
        max_els_1 = unite(indices1a,indices1b);
        max_els_2 = unite(indices2a,indices2b);
    elif len(indices1) > _batch_size_merge and len(indices2) <= _batch_size_merge:
        indices1a, indices1b = split(indices1,True);
        indices1a, max_els_2 = parallel_max_merge(indices1a,indices2);
        indices1b, max_els_2 = parallel_max_merge(indices1b,max_els_2);
        #-----------------------------------------------------------------------------------
        ##parallel_max_merge(indices1a,indices2,results);
        ##indices1a, max_els_2 = results.get();
        #-----------------------------------------------------------------------------------
        ##parallel_max_merge(indices1b,max_els_2,results);
        ##indices1b, max_els_2 = results.get();
        #-----------------------------------------------------------------------------------
        max_els_1            = unite(indices1a,indices1b);
    else:
        indices2a, indices2b = split(indices2,True);
        max_els_1, indices2a = parallel_max_merge(indices1,indices2a);
        max_els_1, indices2b = parallel_max_merge(max_els_1,indices2b);
        #-----------------------------------------------------------------------------------
        ##parallel_max_merge(indices1,indices2a,results);
        ##max_els_1, indices2a = results.get();
        #-----------------------------------------------------------------------------------
        ##parallel_max_merge(max_els_1,indices2b,results);
        ##max_els_1, indices2b = results.get();
        #-----------------------------------------------------------------------------------
        max_els_2            = unite(indices2a,indices2b);
    return max_els_1, max_els_2;
    ##returns.put((max_els_1,max_els_2,));

##def parallel_max_els(indices,returns):
def parallel_max_els(indices):
    ##manager = MP.Manager();
    ##results = manager.Queue();
    max_els = None;
    if len(indices) < _batch_size:
        print('Getting minimum batch size max els...');
        max_els = get_max_els(indices);
        print('Done getting minimum batch size max els.');
    else:
        indices1, indices2 = split(indices,True); # every second itemset
        print('Starting subprocesses with',len(indices1),len(indices2),'indices respectively...');
        with NestablePool(processes=2) as pool:
            max_els1, max_els2 = pool.starmap(parallel_max_els,[(indices1,),(indices2,)]);
        #-----------------------------------------------------------------------------------
        ##p1      = MP.Process(target=parallel_max_els,args=(indices1,results,));
        ##p2      = MP.Process(target=parallel_max_els,args=(indices2,results,));
        ##p1.start(); p2.start();
        ##p1.join();  p2.join();
        ##max_els1, max_els2 = results.get(), results.get();
        #-----------------------------------------------------------------------------------
        print('Done with subprocesses. Gave', len(max_els1),len(max_els2),'max-els respectively.');
        max_els1, max_els2 = parallel_max_merge(max_els1,max_els2);
        #-----------------------------------------------------------------------------------
        ##parallel_max_merge(max_els1,max_els2,results);
        ##max_els1, max_els2 = results.get();
        #-----------------------------------------------------------------------------------
        max_els            = unite(max_els1,max_els2);
    return max_els;
    ##returns.put(max_els);


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
input('Enter to continue...');
#------------------------------------------------------------
C_global              = C;
F_global              = setsOf;
_batch_size           = 10000000;
_batch_size_merge     = 5000000;
indices_1             = np.array(list(range(1,len(C),35)));
indices_2             = np.array(list(range(2,len(C),35)));
indices_both          = np.sort(np.concatenate([indices_1,indices_2]));
#indices_1_,indices_2_ = split(indices_both,False);


max_els1  = get_max_els(indices_1);
max_els2  = get_max_els(indices_2);
max_els   = get_max_els(indices_both);
max_els_  = unite(*merge_max_els(max_els1,max_els2)); 
max_els__ = parallel_max_els(indices_both);           #This is different, and differs also when changing merge_max_els tp parallel_max_merge
#------------------------------------------------------------
##manager = MP.Manager();
##results = manager.Queue();
##parallel_max_els(indices_both,results);
##max_els__ = results.get();
all_indices = np.arange(1,len(C),1);
#------------------------------------------------------------
max_els__ = parallel_max_els(all_indices);
#------------------------------------------------------------
subsets__ = np.array(sorted(list(set(all_indices) - set(max_els__))));
#------------------------------------------------------------
only_supersets = set().union(*[tup[1] for tup in find_supersets(subsets__,max_els__)]);
#------------------------------------------------------------
singletons = set(max_els__) - set(only_supersets);
#------------------------------------------------------------
supersets = set().union(*[tup[1] for tup in find_supersets(subsets__,all_indices)]);
#------------------------------------------------------------
super_subs = set(subsets__) & set(supersets);
#------------------------------------------------------------
only_subsets = set(subsets__) - super_subs;
#------------------------------------------------------------
min_el_map = defaultdict(set);
for tup in find_supersets(only_subsets,supersets):
    min_el_map[tup[0]] |= tup[1];
#------------------------------------------------------------
rows, cols    = zip(*[(fro,to,) for fro in min_el_map for to in min_el_map[fro]]); max_index = max(all_indices);
min_el_matrix = csr((np.ones(len(rows),dtype=bool),(rows,cols,)), dtype=bool, shape=(max_index+1,max_index+1,) );
num_c, labels = connected_components(min_el_matrix,directed=False);
#------------------------------------------------------------
label2size = Counter(labels);
comp_sizes = Counter(label2size.values());
#------------------------------------------------------------
components    = [set() for i in range(num_c)];
for index in range(len(labels)):
    components[labels[index]].add(index);
components = [components[i] for i in range(len(components)) if len(components[i]) > 1];
rep2mentionIndex = defaultdict(set);
for mentionIndex in range(len(mention2repIndex)):
    rep2mentionIndex[mention2repIndex[mentionIndex]].add(mentionIndex);
OUT = open('components_test.txt','w');
for component in components: #TODO: Currently we do not have all mentionIDs that have the same representation as duplicates
    OUT.write(' '.join([str(mentionIndex) for repIndex in component for mentionIndex in rep2mentionIndex[repIndex]])+'\n');
OUT.close();
#TODO:  Something wrong with the indices. Cannot reproduce the subsets in the sqlite database with real strings...
#------------------------------------------------------------

