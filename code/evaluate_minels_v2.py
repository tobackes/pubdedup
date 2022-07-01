import sys
import sqlite3
import numpy as np
from scipy.sparse import csr_matrix as csr
from collections import Counter, defaultdict
import random

_minel_DB = "institutions_components.db"                         #sys.argv[1];
_repre_DB = "representations_institutions_v2/representations.db" #sys.argv[2];
_feats_DB = "representations_institutions_v2/features.db"        #sys.argv[3];

#------------------------------------------------------------------------------------------------------------------------
def transitive_reduction(M,max_depth):
    edges     = set_diagonal(M,csr(np.zeros(M.shape[0],dtype=bool)[:,None]));
    reduction = edges.copy();
    num, i    = 1,2;
    while num > 0 and i <= max_depth+1:
        new        = edges**i;
        num        = len(new.nonzero()[0]);
        reduction  = reduction > new;
        #print('...',i,':',num;
        i += 1;
        reduction.eliminate_zeros();
        if reduction.diagonal().sum() > 0:
            print('WARNING: Cycles in input matrix!');
    return set_diagonal(reduction,csr(M.diagonal()[:,None])).astype(bool);

def set_diagonal(matrix,new): #WARNING: new is expected to be sparse csr matrix (as opposed to what is expected in set_new)
    matrix.eliminate_zeros(); new.eliminate_zeros();
    rows, cols         = matrix.nonzero();
    data               = matrix.data;
    old                = rows!=cols;
    rows_old, cols_old = rows[old], cols[old];
    data_old           = data[old];
    rows_cols_new      = new.nonzero()[0];
    data_new           = new.data;
    cols_, rows_       = np.concatenate([cols_old,rows_cols_new],0), np.concatenate([rows_old,rows_cols_new],0);
    data_              = np.concatenate([data_old,data_new],0);
    return csr((data_,(rows_,cols_)),shape=matrix.shape);
#------------------------------------------------------------------------------------------------------------------------
print('Preprocessing...');
#------------------------------------------------------------------------------------------------------------------------
_con_minel = sqlite3.connect(_minel_DB);
_con_repre = sqlite3.connect(_repre_DB);
_con_feats = sqlite3.connect(_feats_DB);
_cur_minel = _con_minel.cursor();
_cur_repre = _con_repre.cursor();
_cur_feats = _con_feats.cursor();
#------------------------------------------------------------------------------------------------------------------------
minel_combs       = dict();
minel2bfdID       = dict();
comb2repIDIndeces = defaultdict(set);
repIDIndex2bfdID  = dict();
non_singletons    = set();
#------------------------------------------------------------------------------------------------------------------------
#-GET ALL REPRESENTATIONS THAT ARE UNDER A MINEL IN THE MAPPING----------------------------------------------------------
_cur_minel.execute("SELECT repIDIndex,GROUP_CONCAT(minel) FROM repIDIndex2minel GROUP BY repIDIndex");

for repIDIndex,minel_str in _cur_minel:
    comb            = ','.join(sorted(minel_str.split(',')));
    repID           = _cur_feats.execute("SELECT repID   FROM index2repID     WHERE repIDIndex=?",(repIDIndex,)).fetchall()[0][0];
    rep_freq, bfdID = _cur_repre.execute("SELECT freq,id FROM representations WHERE repID=?"     ,(repID,     )).fetchall()[0];
    if not bfdID in minel_combs:
        minel_combs[bfdID] = Counter();
    minel_combs[bfdID][comb] += rep_freq;
    for minel in comb.split(','):
        if not minel in minel2bfdID:
            minel2bfdID[minel] = Counter();
        minel2bfdID[minel][bfdID] += rep_freq;
    non_singletons.add(repID);
    comb2repIDIndeces[comb].add(repIDIndex);
    repIDIndex2bfdID[repIDIndex] = bfdID;
#------------------------------------------------------------------------------------------------------------------------
#-GET ALL THE MINELS AS REPRESENTATIONS UNDER THEMSELVES-----------------------------------------------------------------
# Basically we just need to know all the missing repIDs because later we add the diagonal anyway...
_cur_minel.execute("SELECT DISTINCT minel FROM repIDIndex2minel");

for row in _cur_minel:
    comb,repIDIndex = str(row[0]),row[0];
    repID           = _cur_feats.execute("SELECT repID   FROM index2repID     WHERE repIDIndex=?",(repIDIndex,)).fetchall()[0][0];
    rep_freq, bfdID = _cur_repre.execute("SELECT freq,id FROM representations WHERE repID=?"     ,(repID,     )).fetchall()[0];
    if not bfdID in minel_combs:
        minel_combs[bfdID] = Counter();
    minel_combs[bfdID][comb] += rep_freq;
    for minel in comb.split(','):
        if not minel in minel2bfdID:
            minel2bfdID[minel] = Counter();
        minel2bfdID[minel][bfdID] += rep_freq;
    non_singletons.add(repID);
    comb2repIDIndeces[comb].add(repIDIndex);
    repIDIndex2bfdID[repIDIndex] = bfdID;
#------------------------------------------------------------------------------------------------------------------------
#-GET ALL THE REPRESENTATIONS THAT ARE NEITHER LISTED UNDER A MINEL IN THE MAPPING NOR MINELS THEMSELVES AS SINGLETONS---
# Basically we just need to know all the missing repIDs because later we add the diagonal anyway...
_cur_feats.execute("SELECT repID,repIDIndex FROM index2repID");

for repID,repIDIndex in _cur_feats: #TODO: Something has to be wrong with non_singletons as a lot of specific repID are not in it...
    rep_freq, bfdID = _cur_repre.execute("SELECT freq,id FROM representations WHERE repID=?",(repID,)).fetchall()[0];
    if not repID in non_singletons:  # minels or supersets already observed
        #input(repID.replace('None+++','').replace('+++None',''));
        if not bfdID in minel_combs:
            minel_combs[bfdID] = Counter();
        minel_combs[bfdID][str(repIDIndex)] += rep_freq;
        for minel in comb.split(','):
            if not minel in minel2bfdID:
                minel2bfdID[minel] = Counter();
            minel2bfdID[minel][bfdID] += rep_freq;
        comb2repIDIndeces[str(repIDIndex)].add(repIDIndex);
    repIDIndex2bfdID[repIDIndex] = bfdID;
#------------------------------------------------------------------------------------------------------------------------
print('Done preprocessing.');
#------------------------------------------------------------------------------------------------------------------------
'''
# NOTE: This has been evaluated as we get the same results when we add redundant information in the find_components_disk.
# NOTE: The last two of the three above steps are required if the components and minels are stored non-redundant!

TPs = 0;
Ts  = 0;
Rs  = 0;
FNs = dict();

for bfdID in minel_combs:
    index2comb = list(minel_combs[bfdID].keys());
    comb2index = {index2comb[i]:i for i in range(len(index2comb))};

    minel2combs = defaultdict(set);
    for comb in index2comb:
        for minel in comb.split(','):
            minel2combs[minel].add(comb);

    comb2supersets = defaultdict(set);
    for comb in index2comb:
        supersets            = set.intersection(*[minel2combs[minel] for minel in comb.split(',')]);
        comb2supersets[comb] = supersets;

    edges     = [(comb2index[comb],comb2index[superset],) for comb in comb2supersets for superset in comb2supersets[comb]];
    #edges    += [(edge[1],edge[0]) for edge in edges]; # This cannot be used before transitive closure if the latter is applied!!!
    rows,cols = zip(*edges);                                           #TODO: This misses connections via combs outside the bfdID
    size      = max(rows+cols)+1; # Can't we just use len(index2comb)? #TODO: We should use the global reach and only use the bfdID indices as below
    reach     = csr((np.ones(len(edges),dtype=bool),(rows,cols,)),shape=(size,size,)); # Reach is edges from subsets to supersets, including self
    reach     = reach.T.dot(reach);                                                    # Add the overlapping sets
    reach     = reach+reach.T;                                                         # Add the backwards pairs for subset->superset
    max_combs = max((len(comb.split(',')) for comb in comb2index));
    freq      = np.array([minel_combs[bfdID][index2comb[i]] for i in range(len(index2comb))]);
    alle      = freq.sum();
    TP        = freq.dot(reach.dot(freq[:,None]))[0];
    T         = alle**2;
    TPs      += TP;
    Ts       += T;
    Rs       += TP/T;
    FNs[bfdID]= set([(index2comb[i],index2comb[j],) for i in range(min(3,size)) for j in range(max(0,size-3),size) if not reach[i,j]]);

    print('bfdID:',bfdID,'| combs:',len(index2comb),'| minels:',len(minel2combs),'| ments:',alle,'| rec:',int(100*TP/T),'| TP:',TP,'| T:',T,'| depth:',max_combs);
    #print(','.join(minel2combs.keys()));

print('Overall micro average recall:',int(100*TPs/Ts));
print('Overall macro average recall:',int(100*Rs/len(minel_combs)));
print('TP:',TPs,'T:',Ts);
'''
#------------------------------------------------------------------------------------------------------------------------
#-GET THE MINEL COMBS REGARDLESS OF BFD-ID-------------------------------------------------------------------------------
all_combs = Counter();
for bfdID in minel_combs:
    for comb in minel_combs[bfdID]:
        all_combs[comb] += minel_combs[bfdID][comb];
#for comb in all_combs:
#    all_combs[comb] /= len(comb.split(','));#NO!
#------------------------------------------------------------------------------------------------------------------------
FPs = dict();

index2comb = list(all_combs.keys());
comb2index = {index2comb[i]:i for i in range(len(index2comb))};

minel2combs = defaultdict(set);
for comb in index2comb:
    for minel in comb.split(','):
        minel2combs[minel].add(comb);

comb2supersets = defaultdict(set);
for comb in comb2index:
    supersets            = set.intersection(*[minel2combs[minel] for minel in comb.split(',')]);
    comb2supersets[comb] = supersets;

freqs     = np.array([all_combs[index2comb[i]] for i in range(len(index2comb))],dtype=int);
edges     = [(comb2index[comb],comb2index[superset],) for comb in comb2supersets for superset in comb2supersets[comb]];
rows,cols = zip(*edges);
size      = max(rows+cols)+1;
reach     = csr((np.ones(len(edges),dtype=bool),(rows,cols,)),shape=(size,size,));

#------------------------------------------------------------------------------------------------------------------------
input('Enter...');
#------------------------------------------------------------------------------------------------------------------------
TPs = 0;
Ts  = 0;
Rs  = 0;
FNs = dict();

for bfdID in minel_combs:
    combi   = [comb2index[comb] for comb in minel_combs[bfdID]];
    freqs_  = np.array([minel_combs[bfdID][index2comb[i]] for i in range(len(index2comb))],dtype=int); #The freq of this comb in the current bfdID
    #supers  = reach[:,combi];                                           # The supersets
    #supers.setdiag(0);                                                  # The proper supersets
    #reach_  = supers[combi,:] + supers[combi,:].T;                      # The proper subset and supersets within bfdID
    #reach__ = (reach.T[combi,:].dot(supers));                           # The overlapping sets that are not sub/supersets but includes self
    #TP      = reach__.multiply(freqs_[combi][:,None]).dot(freqs_[combi][:,None]).sum(); # Adding the number of pairs from proper sub/supersets
    #TP     += reach_.multiply(freqs_[combi][:,None]).dot(freqs_[combi][:,None]).sum();  # Adding the number of pairs from overlapping and self
    reach_  = (reach.T[combi,:].dot(reach[:,combi]));
    TP      = reach_.multiply(freqs_[combi][:,None]).dot(freqs_[combi][:,None]).sum();
    T       = freqs_[combi].sum()**2;
    R       = TP/T if T >0 else 0;
    TPs    += TP;
    Ts     += T;
    Rs     += R;
    FNs[bfdID] = set([(index2comb[i],index2comb[j],) for i in combi[:min(3,len(combi))] for j in combi[max(0,len(combi)-3):] if not reach[i,j]]);
    print('bfdID:',bfdID,'| combs:',len(combi),'| ments:',freqs_[combi].sum(),'| rec:',int(100*R),'| TP:',TP,'| T:',T);
#------------------------------------------------------------------------------------------------------------------------
print('Overall micro average recall:',int(100*TPs/Ts));
print('Overall macro average recall:',int(100*Rs/len(minel_combs)));
print('TP:',TPs,'T:',Ts);
#------------------------------------------------------------------------------------------------------------------------
input('Enter...');
#------------------------------------------------------------------------------------------------------------------------
_batch = 1000;
Ps     = 0;
i      = 0;
for combi in [np.array(range(j,min(j+_batch,len(all_combs)))) for j in range(0,len(all_combs),_batch)]:
                                                                                # Reach is edges from subsets to supersets, including self
    reach_  = (reach.T[combi,:].dot(reach));                                    # Add the overlapping sets
                                                                                #TODO: Add the backwards pairs for subset->superset
    P       = reach_.multiply(freqs[combi][:,None]).dot(freqs[:,None]).sum();
    Ps     += P;
    i      += 1;
    if i % 10 == 0:
        print(100*_batch*i/len(all_combs),'% (',Ps,')',end='\r');

print(Ps);


'''
edges     = [(comb2index[comb],comb2index[superset],) for comb in comb2supersets for superset in comb2supersets[comb]];
#edges    += [(edge[1],edge[0]) for edge in edges]; # This cannot be used before transitive closure if the latter is applied!!!
rows,cols = zip(*edges);
size      = max(rows+cols)+1;
reach     = csr((np.ones(len(edges),dtype=bool),(rows,cols,)),shape=(size,size,));
reach     = reach.T.dot(reach);
max_combs = max((len(comb.split(',')) for comb in comb2index));
freq      = np.array([all_combs[index2comb[i]] for i in range(len(index2comb))]);
alle      = freq.sum();
P         = freq.dot(reach.dot(freq[:,None]))[0];
all_pairs = alle**2;

TP = sum( (sum( (minel2bfdID[minel][bfdID]**2 for bfdID in minel2bfdID[minel]) ) for minel in minel2bfdID       ) );
T  = sum( (sum( (minel_combs[bfdID][comb] **2 for comb  in minel_combs[bfdID]) ) for comb  in minel_combs[bfdID]) );
#TODO: P  = ???;
'''
FPs     = set();
num     = 0;
reaches = [(a,b) for a,b in zip(*reach.nonzero())];
for i,j in random.sample(reaches,len(reaches)): # TODO: Inefficient
    comb_i       = index2comb[i];
    comb_j       = index2comb[j];
    repIDIndex_i = random.choice(list(comb2repIDIndeces[comb_i]));
    repIDIndex_j = random.choice(list(comb2repIDIndeces[comb_j]));
    bfdID_i      = repIDIndex2bfdID[repIDIndex_i];
    bfdID_j      = repIDIndex2bfdID[repIDIndex_j];
    if bfdID_i != bfdID_j and comb_i != comb_j: # The second part is not required, just to give more interesting examples.
        FPs.add((repIDIndex_i,repIDIndex_j,comb_i,comb_j,bfdID_i,bfdID_j,));
        num += 1;
        if num > 100:
            break;

print('combs:',len(index2comb),'| minels:',len(minel2combs),'| ments:',alle,'| rec:',int(100*TPs/Ts),'| pre:',int(100*TPs/P),'| TP:',TPs,'| T:',Ts,'| P:',P,'| depth:',max_combs);

for bfdID in FNs:
    input(bfdID+'...');
    for comb1,comb2 in FNs[bfdID]:
        feats1 = [_cur_feats.execute("SELECT feat,featGroup from index2feat where featIndex in (select featIndex from features WHERE repIDIndex=?)",(minel,)).fetchall() for minel in comb1.split(',')];
        feats2 = [_cur_feats.execute("SELECT feat,featGroup from index2feat where featIndex in (select featIndex from features WHERE repIDIndex=?)",(minel,)).fetchall() for minel in comb2.split(',')];
        print('============================================================');
        for featlist in feats1:
            print(','.join([attr+':'+val for val,attr in featlist]));
        print('--------------------------VS--------------------------------');
        for featlist in feats2:
            print(','.join([attr+':'+val for val,attr in featlist]));
        print('============================================================');

for repIDIndex1,repIDIndex2,comb1,comb2,bfdID1,bfdID2 in FPs:
    input(comb1+'->'+str(repIDIndex1)+': '+bfdID1+'\n'+comb2+'->'+str(repIDIndex2)+': '+bfdID2+'...');
    feats1 = [_cur_feats.execute("SELECT feat,featGroup from index2feat where featIndex in (select featIndex from features WHERE repIDIndex=?)",(minel,)).fetchall() for minel in comb1.split(',')];
    feats2 = [_cur_feats.execute("SELECT feat,featGroup from index2feat where featIndex in (select featIndex from features WHERE repIDIndex=?)",(minel,)).fetchall() for minel in comb2.split(',')];
    print('============================================================');
    for featlist in feats1:
        print(','.join([attr+':'+val for val,attr in featlist]));
    print('--------------------------VS--------------------------------');
    for featlist in feats2:
        print(','.join([attr+':'+val for val,attr in featlist]));
    print('============================================================');
