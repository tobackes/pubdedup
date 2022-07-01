import sqlite3
import sys
import numpy as np
from collections import defaultdict
import time

_inDB  = sys.argv[1];

con   = sqlite3.connect(_inDB);
cur   = con.cursor();

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

buckets = [[] for i in range(100)];
for i in range(len(featsOf)):
    buckets[len(featsOf[i])].append(i);
sorting = [index for bucket in buckets for index in bucket];
del buckets;

def is_subset(a,b): #a is subset of b, so all elements of a must be in b
    if len(a) >= len(b):
        return False;
    b_ = set(b);
    for el in a:
        if not el in b_:
            return False;
    return True;

featSize  = np.array([len(a) for a in setsOf]);
featOrder = np.argsort(featSize);
for i in range(len(featsOf)):
    if i % 1000000 == 0:
        print(i);
    if len(featsOf[i]) > 0:
        featsOf[i] = featsOf[i][np.argsort(featOrder[featsOf[i]])];


D                = featsOf[sorting];  # input itemsets
#index2repID      = np.array(['/'.join([str(f) for f in D[i]]) for i in range(len(D))]) #gets too large!
D_               = pad_to_dense(D); #TODO: I realized that this ignores the fact that there is probably a feature 0!
_, unique        = np.unique(D_,axis=0,return_index=True); #TODO: Could be modified to obtain lexicographic order on features regarding frequency of occurance
C                = D[unique]; #TODO: Check if the sorting is preserving the cardinality sort
mention2repIndex = get_mention2repIndex(unique,len(D));

repsOf = defaultdict(set);
for repIndex in range(len(C)):
    for feat in C[repIndex]:
        repsOf[feat].add(repIndex);
featSize_   = np.array([len(repsOf[i]) for i in range(len(setsOf))])
singfeats   = (featSize_==1).nonzero()[0];
singfeatset = set(singfeats);

C_mod = np.array([np.array([feat for feat in rep if not feat in singfeatset]) for rep in C],dtype=object);
#TODO: Modify all representations and resort, etc.

def get_mention2repIndex(unique_,len_C):
    unique = sorted(unique_);
    return np.array([i for i in range(len(unique)-1) for mentionIndex in range(unique[i],unique[i+1])] + [len(unique)-1 for mentionIndex in range(unique[-1],len_C)]);

def pad_to_dense(M,maxlen=None):
    maxlen = max(len(r) for r in M) if maxlen==None else maxlen;
    Z      = np.zeros((len(M), maxlen));
    for enu, row in enumerate(M):
        if len(row) > 0:
            Z[enu,-len(row):] += row;
    return Z;

def get_min_els(C,indices): # assuming that itemsets in D are sorted by cardinality and items in itemsets by occurance frequency
    D = C[indices];         # TODO: Correct by using subset not hasSups and return minimal elements
    #-----------------------------------------------------------------------------
    hasSups = defaultdict(set);
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
                if not k in hasSups:            #if current reference itemset has no known supersets
                    S = D[k];                   #get reference itemset from reference itemset_index
                    if len(S) > len(D[i])-j+1:  #if the length of the current reference itemset is larger than the length of the itemset -j+1
                        break;
                    if is_subset(S,D[i]):       #if itemset is superset of reference itemset
                        hasSups[k].add(i);      #add itemset index to supersets of reference itemset
        if len(D[i]) > c:
            for k in B:
                S = D[k];
                O[S[0]].append(k);
            B = set([]);
            c = len(D[i]);
        B.add(i);
    #----------------------------------------------------------------------------- #TODO: Not sure everything that is a superset is in hasSups
    max_els   = np.array(sorted([indices[i] for i in range(len(D)) if not i in hasSups]));   #a maxel is anything that has no supersets
    supersets = set([j for i in hasSups for j in hasSups[i]]);
    min_els   = [i for i in range(len(D)) if not i in supersets]; #a minel is anything that is not a superset
    min2els   = {indices[min_el]:set(indices[list(hasSups[min_el])]) for min_el in min_els};
    min_els   = np.array(sorted([indices[min_el] for min_el in min_els]));
    el2mins   = defaultdict(set);
    for min_el, els in min2els.items():
        for el in els:
            el2mins[el].add(min_el);
    #-----------------------------------------------------------------------------
    return min_els, min2els, el2mins;

def find_subsets(C,indices): # assuming that itemsets in D are sorted by cardinality and items in itemsets by occurance frequency
    D = C[indices];
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
    #----------------------------------------------------------------------------- #TODO: Not sure everything that is a superset is in hasSups
    return np.array(sorted([indices[subset] for subset in subsets]));

def merge(C,min_indices_1,min_indices_2,min2els_1,min2els_2):
    hasSups    = defaultdict(set);                            #TODO: correct by replacing hasSups
    supersets1 = set();
    supersets2 = set();
    #----------------------------------------------------------------------------- This is now redundant
    print('Building help structure 1');
    struct  = dict();
    for min_index in min_indices_2:
        size  = len(C[min_index]);
        if not size in struct:
            struct[size] = dict();
        for feat in C[min_index]:
            if feat in struct[size]:
                struct[size][feat].append(min_index);
            else:
                struct[size][feat] = [min_index];
    #-----------------------------------------------------------------------------
    print('Looking for supersets of collection 1 in collection 2');
    for min_index_1 in min_indices_1: #looking for supersets of itemsets1 in itemsets2
        size_1 = len(C[min_index_1]);
        first  = C[min_index_1][0];
        for size_2 in struct:
            if size_2 > size_1 and first in struct[size_2]: #supersets must be larger and contain all elements -- also the least frequent / first one
                for min_index_2 in struct[size_2][first]:
                    if is_subset(C[min_index_1],C[min_index_2]):
                        hasSups[min_index_1].add(min_index_2);
                        supersets1.add(min_index_2);
    #----------------------------------------------------------------------------- This is now redundant
    #print('Supersets in min_els2:',supersets1);
    print('Building help structure 2');
    struct  = dict();
    for min_index in min_indices_1:
        #if min_index in hasSups:
        #    continue; #those itemsets1 for which we have already found supersets in itemsets2 cannot be supersets of anything in itemsets2
        size  = len(C[min_index]);
        if not size in struct:
            struct[size] = dict();
        for feat in C[min_index]:
            if feat in struct[size]:
                struct[size][feat].append(min_index);
            else:
                struct[size][feat] = [min_index];
    #-----------------------------------------------------------------------------
    print('Looking for supersets of collection 2 in collection 1');
    for min_index_2 in min_indices_2: #looking for supersets of itemsets2 in itemsets1
        #if min_index_2 in supersets:
        #    continue; #those itemsets2 for which we have already found subsets in itemsets1 cannot be subsets of anything in itemsets1
        size_2 = len(C[min_index_2]);
        first  = C[min_index_2][0];
        for size_1 in struct:
            if size_1 > size_2 and first in struct[size_1]: #supersets must be larger and contain all elements -- also the least frequent / first one
                for min_index_1 in struct[size_1][first]:
                    if is_subset(C[min_index_2],C[min_index_1]):
                        hasSups[min_index_2].add(min_index_1);
                        supersets2.add(min_index_1);
    #----------------------------------------------------------------------------- #TODO: The below is too slow!
    #print('Supersets in min_els1:',supersets2);
    print('Combining min_el information');
    supersets = supersets1 | supersets2;
    min_els1  = [min_index_1 for min_index_1 in min_indices_1 if not min_index_1 in supersets];
    min_els2  = [min_index_2 for min_index_2 in min_indices_2 if not min_index_2 in supersets];
    min_els   = np.array(sorted(list(set(min_els1) | set(min_els2))));
    min2els   = {min_el:set() for min_el in min_els};
    for min_el in min_els:
        if min_el in min2els_1:
            min2els[min_el] |= min2els_1[min_el];
        if min_el in min2els_2:
            min2els[min_el] |= min2els_2[min_el];
        if min_el in hasSups:
            min2els[min_el] |= hasSups[min_el];
    el2mins = defaultdict(set);
    for min_el, els in min2els.items():
        for el in els:
            el2mins[el].add(min_el);
    #-----------------------------------------------------------------------------
    return min_els, min2els, el2mins;

def merge_(C,min_indices_1,min_indices_2,min2els_1,min2els_2): # This is from the paper but it is way too slow!
    hasSubs   = defaultdict(set);
    supersets = set();
    print('Looking for supersets');
    #-----------------------------------------------------------------------------
    for min_index_1 in min_indices_1:
        for min_index_2 in min_indices_2:
            if not min_index_2 in hasSubs:
                if is_subset(C[min_index_2],C[min_index_1]):
                    hasSubs[min_index_1].add(min_index_2);
                    break;
                if is_subset(C[min_index_1],C[min_index_2]):
                    hasSubs[min_index_2].add(min_index_1);
    #----------------------------------------------------------------------------- #TODO: The below is too slow!
    print('Combining min_el information');
    min_els1 = [min_index_1 for min_index_1 in min_indices_1 if not min_index_1 in supersets];
    min_els2 = [min_index_2 for min_index_2 in min_indices_2 if not min_index_2 in supersets];
    min_els  = np.array(sorted(list(set(min_els1) | set(min_els2))));
    min2els  = {min_el:set() for min_el in min_els};
    for min_el in min_els:
        if min_el in min2els_1:
            min2els[min_el] |= min2els_1[min_el];
        if min_el in min2els_2:
            min2els[min_el] |= min2els_2[min_el];
        if min_el in hasSups:
            min2els[min_el] |= hasSups[min_el];
    el2mins = defaultdict(set);
    for min_el, els in min2els.items():
        for el in els:
            el2mins[el].add(min_el);
    #-----------------------------------------------------------------------------
    return min_els, min2els, el2mins;

def parallel_min_els(C,indices): #TODO: Does not work yet!!!
    print(len(indices));
    if len(indices) < _batch_size:
        return get_min_els(C,indices);
    else:
        min_indices_1                = indices[list(range(0,len(indices),2))]; # every second itemset
        min_indices_2                = indices[list(range(1,len(indices),2))]; # every second itemset
        min_els1, min2els1, el2mins1 = parallel_min_els(C,min_indices_1); #TODO: spawn new process for this
        min_els2, min2els2, el2mins2 = parallel_min_els(C,min_indices_2); #TODO: spawn new process for this
        print(len(min_els1),len(min_els2));
        return merge(C,min_els1,min_els2,min2els1,min2els2);

indices_1                       = np.array(range(1267803+  0,len(C),350));
indices_2                       = np.array(range(1267803+100,len(C),350));
indices_both                    = np.sort(np.concatenate([indices_1,indices_2]));
min_els1 , min2els1 , el2mins1  = get_min_els(C,indices_1);
min_els2 , min2els2 , el2mins2  = get_min_els(C,indices_2);
min_els  , min2els  , el2mins   = get_min_els(C,indices_both);
min_els_ , min2els_ , el2mins_  = merge(C,min_els1,min_els2,min2els1,min2els2);
min_els__, min2els__, el2mins__ = parallel_min_els(C,indices_both);

subsets1 = find_subsets(C,indices_1);
subsets2 = find_subsets(C,indices_2);
subsets  = find_subsets(C,np.sort(np.concatenate([subsets1,subsets2])));


#----------------------------------------------------------------------------------------------
#TODO: It seems to error-prone to try and modivy the algorithm towards finding minimal elements
#----------------------------------------------------------------------------------------------
#TODO: Currently, we have for each subset a list of all detected supersets (not necessarily all)
#      We only cared about finding the itemsets that have no supersets and those are guaranteed not to occur in hasSups
#      A minimal itemset can also be a maximal itemset, but these are only size-1 components which we do not care about
#      Otherwise, a minimal itemset cannot be a maximal itemset, so the set of minimal itemsets must be a subset of what hasSups
#      A minimal itemset is an itemset which hasSups but is not in any set of hasSups
#      That would be set(hasSups.keys()) - set([j for i in hasSups for j in hasSups[i]]);
#      There should be more space-efficient solutions for this task, but this seems quite certainly correct
