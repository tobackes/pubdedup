import sqlite3
import sys
import numpy as np
from collections import defaultdict
from functools import reduce
import time
import multiprocessing as MP

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

def get_mention2repIndex(unique_,len_C):
    unique = sorted(unique_);
    return np.array([i for i in range(len(unique)-1) for mentionIndex in range(unique[i],unique[i+1])] + [len(unique)-1 for mentionIndex in range(unique[-1],len_C)]);

def pad_to_dense(M,maxlen=None):
    maxlen = max(len(r) for r in M) if maxlen==None else maxlen;
    Z      = -np.ones((len(M), maxlen));
    for enu, row in enumerate(M):
        if len(row) > 0:
            Z[enu,-len(row):] += row+1;
    return Z;

def get_unique(D):
    D_        = pad_to_dense(D);
    _, unique = np.unique(D_,axis=0,return_index=True); #TODO: Could be modified to obtain lexicographic order on features regarding frequency of occurance
    return unique;

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

def get_max_els(indices): # assuming that itemsets in D are sorted by cardinality and items in itemsets by occurance frequency
    subsets = find_subsets(indices);
    max_els = np.array(sorted([index for index in indices if not index in subsets]));
    return max_els;

def find_subsets(indices): # assuming that itemsets in D are sorted by cardinality and items in itemsets by occurance frequency
    D = C_global[indices];
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
    indices2_ = set(indices2);
    supersets = np.array([]);
    t          = time.time();
    for i in range(len(indices1)):
        if i % 100000 == 0:
            print(i,time.time()-t); t = time.time();
        #supersets |= set.intersection(*[(set(F_global[feat])&indices2_)-supersets for feat in C_global[indices1[i]]]);
        #TODO: Instead use ufunc reduce on np.intersect1d but order overlaps by length first
        overlaps   = sorted([reduce(np.setdiff1d,(F_global[feat],indices1,supersets)) for feat in C_global[indices1[i]]],key=len);
        supersets_ = reduce(np.intersect1d, overlaps);
        supersets  = np.concatenate(supersets,supersets_);
    return np.array(sorted(list(supersets)));

    

def merge_max_els(indices1,indices2): #TODO: It is likely that the problem is in this function!
    subsets1 = set();
    subsets2 = set();
    #----------------------------------------------------------------------------- This is now redundant
    print('Building help structure 1'); t = time.time();
    struct  = dict();
    for index2 in indices2:
        size = len(C_global[index2]);
        if not size in struct:
            struct[size] = dict();
        for feat in C_global[index2]:
            if feat in struct[size]:
                struct[size][feat].add(index2);
            else:
                struct[size][feat] = set([index2]);
    #-----------------------------------------------------------------------------
    print('Took',time.time()-t,'seconds.');
    print('Looking for supersets of collection 1 in collection 2'); t = time.time();
    for index1 in indices1: #looking for subsets of itemsets1 in itemsets2
        first = C_global[index1][0];                                    #alternative
        for size2 in sorted([size for size in struct if size > len(C_global[index1]) and first in struct[size]],reverse=True):
            #overlap = [struct[size2][feat] if feat in struct[size2] else set() for feat in C_global[index1]];
            #if len(overlap) > 0 and set.intersection(*overlap) != set([]):
            #    subsets1.add(index1);
            for index2 in struct[size2][first]:                     #alternative
                if is_subset(C_global[index1],C_global[index2]):    #alternative
                    subsets1.add(index1);                           #alternative
                    break;
    #----------------------------------------------------------------------------- This is now redundant
    print('Found',len(subsets1),'subsets in max_els1.'); print('Took',time.time()-t,'seconds.');
    print('Building help structure 2'); t = time.time();
    struct  = dict();
    for index1 in indices1:
        size = len(C_global[index1]);
        if not size in struct:
            struct[size] = dict();
        for feat in C_global[index1]:
            if feat in struct[size]:
                struct[size][feat].add(index1);
            else:
                struct[size][feat] = set([index1]);
    #-----------------------------------------------------------------------------
    print('Took',time.time()-t,'seconds.');
    print('Looking for supersets of collection 2 in collection 1'); t = time.time();
    for index2 in indices2: #looking for subsets of itemsets2 in itemsets1
        first = C_global[index2][0];                                    #alternative
        for size1 in sorted([size for size in struct if size > len(C_global[index2]) and first in struct[size]],reverse=True):
            #overlap = [struct[size1][feat] if feat in struct[size1] else set() for feat in C_global[index2]];
            #if len(overlap) > 0 and set.intersection(*overlap) != set([]):
            #    subsets2.add(index2);
            for index1 in struct[size1][first]:                     #alternative
                if is_subset(C_global[index2],C_global[index1]):    #alternative
                    subsets2.add(index2);                           #alternative
                    break;
    #-----------------------------------------------------------------------------
    print('Found',len(subsets2),'subsets in max_els2.'); print('Took',time.time()-t,'seconds.');
    print('Combining max_el information'); t = time.time();
    #subsets = subsets1 | subsets2;
    #max_els = np.array([max_el for max_el in np.sort(np.concatenate([indices1,indices2])) if not max_el in subsets]);
    max_els1 = [max_el for max_el in indices1 if not max_el in subsets1];
    max_els2 = [max_el for max_el in indices2 if not max_el in subsets2];
    print('Took',time.time()-t,'seconds.');
    #-----------------------------------------------------------------------------
    return max_els1, max_els2;

def parallel_max_merge(indices1,indices2):
    max_els_1, max_els_2 = [None,None];
    if len(indices1) <= _batch_size_merge and len(indices2) <= _batch_size_merge:
        max_els_1, max_els_2 = merge_max_els(indices1,indices2);
    elif len(indices1) > _batch_size_merge and len(indices2) > _batch_size_merge:
        indices1a, indices1b = split(indices1,True);
        indices2a, indices2b = split(indices2,True);
        with NestablePool(processes=2) as pool:
            [indices1a,indices2a], [indices1b,indices2b] = pool.starmap(parallel_max_merge,[(indices1a,indices2a,),(indices1b,indices2b,)]);
            [indices1a,indices2b], [indices1b,indices2a] = pool.starmap(parallel_max_merge,[(indices1a,indices2b,),(indices1b,indices2a,)]);
        max_els_1 = unite(indices1a,indices1b);
        max_els_2 = unite(indices2a,indices2b);
    elif len(indices1) > _batch_size_merge and len(indices2) <= _batch_size_merge:
        indices1a, indices1b = split(indices1,True);
        indices1a, max_els_2 = parallel_max_merge(indices1a,indices2);
        indices1b, max_els_2 = parallel_max_merge(indices1b,max_els_2);
        max_els_1            = unite(indices1a,indices1b);
    else:
        indices2a, indices2b = split(indices2,True);
        max_els_1, indices2a = parallel_max_merge(indices1,indices2a);
        max_els_1, indices2b = parallel_max_merge(max_els_1,indices2b);
        max_els_2            = unite(indices2a,indices2b);
    return max_els_1, max_els_2;

def unite(indices1,indices2):
    return np.array(sorted(list(set(indices1)|set(indices2))));

def split(indices,stratified=False):
    if stratified:
        return indices[list(range(0,len(indices),2))], indices[list(range(1,len(indices),2))];
    return indices[:int(len(indices)/2)], indices[int(len(indices)/2):];

def parallel_max_els(indices): #TODO: Need to reduce the space of the passed index arrays!
    print(len(indices));
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
        print('Done with subprocesses. Gave', len(max_els1),len(max_els2),'max-els respectively.');
        max_els1, max_els2 = parallel_max_merge(max_els1,max_els2);
        max_els            = unite(max_els1,max_els2);
    return max_els;


#------------------------------------------------------------
con              = sqlite3.connect(_inDB);
cur              = con.cursor();
featsOf          = get_featsOf(cur);
setsOf           = get_setsOf(cur);
D                = get_sets(featsOf);
unique           = get_unique(D);
mention2repIndex = get_mention2repIndex(unique,len(D));
C                = D[unique];
#sorting          = get_cardinality_sorting(C); #unnecessary because unique sorts by cardinality, but this might be a good presorting for speedup
#C                = C[sorting];
C                = sort_reps(C,setsOf); #This might also go before make_unique as it then affects the itemset order within same cardinality but is more expensive
#------------------------------------------------------------

#------------------------------------------------------------
C_global              = C;
_batch_size           = 5000000;
_batch_size_merge     = 2500000;
indices_1             = np.array(list(range(1,len(C),10)));
indices_2             = np.array(list(range(2,len(C),10)));
indices_both          = np.sort(np.concatenate([indices_1,indices_2]));
#indices_1_,indices_2_ = split(indices_both,False);


max_els1  = get_max_els(indices_1);
max_els2  = get_max_els(indices_2);
max_els   = get_max_els(indices_both);
max_els_  = unite(*merge_max_els(max_els1,max_els2)); 
max_els__ = parallel_max_els(indices_both);           #This is different, and differs also when changing merge_max_els tp parallel_max_merge
#------------------------------------------------------------

#------------------------------------------------------------
subsets1 = find_subsets(C,indices_1);
subsets2 = find_subsets(C,indices_2);
subsets  = find_subsets(C,np.sort(np.concatenate([subsets1,subsets2])));
#------------------------------------------------------------
