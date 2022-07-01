import sys
import sqlite3
import numpy as np
from scipy.sparse import csr_matrix as csr
from collections import Counter, defaultdict

_minel_DB = "institutions_components.db"                         #sys.argv[1];
_repre_DB = "representations_institutions_v2/representations.db" #sys.argv[2];
_feats_DB = "representations_institutions_v2/features3.db"        #sys.argv[3];

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
minel_combs    = dict();
bfdID2combs    = dict();
non_singletons = set();
#------------------------------------------------------------------------------------------------------------------------
#-GET ALL REPRESENTATIONS THAT ARE UNDER A MINEL IN THE MAPPING----------------------------------------------------------
_cur_minel.execute("SELECT repIDIndex,GROUP_CONCAT(minel) FROM repIDIndex2minel GROUP BY repIDIndex");

for repIDIndex,minel_str in _cur_minel:
    comb            = ','.join(sorted(minel_str.split(','))); #TODO: Should there only be used the minels that are in the bfdID!
    repID           = _cur_feats.execute("SELECT repID   FROM index2repID     WHERE repIDIndex=?",(repIDIndex,)).fetchall()[0][0];
    rep_freq, bfdID = _cur_repre.execute("SELECT freq,id FROM representations WHERE repID=?"     ,(repID,     )).fetchall()[0];
    if not bfdID in minel_combs:
        minel_combs[bfdID] = Counter();
        bfdID2combs[bfdID] = set();
    minel_combs[bfdID][comb] += rep_freq;
    bfdID2combs[bfdID].add(comb);
#------------------------------------------------------------------------------------------------------------------------
#-GET ALL THE MINELS AS REPRESENTATIONS UNDER THEMSELVES-----------------------------------------------------------------
#TODO: Basically we just need to know all the missing repIDs because later we add the diagonal anyway...
_cur_minel.execute("SELECT DISTINCT minel FROM repIDIndex2minel");

for row in _cur_minel:
    comb,repIDIndex = str(row[0]),row[0];
    repID           = _cur_feats.execute("SELECT repID   FROM index2repID     WHERE repIDIndex=?",(repIDIndex,)).fetchall()[0][0];
    rep_freq, bfdID = _cur_repre.execute("SELECT freq,id FROM representations WHERE repID=?"     ,(repID,     )).fetchall()[0];
    if not bfdID in minel_combs:
        minel_combs[bfdID] = Counter();
        bfdID2combs[bfdID] = set();
    minel_combs[bfdID][comb] += rep_freq;
    bfdID2combs[bfdID].add(comb);
    non_singletons.add(repID);
#------------------------------------------------------------------------------------------------------------------------
#-GET ALL THE REPRESENTATIONS THAT ARE NEITHER LISTED UNDER A MINEL IN THE MAPPING NOR MINELS THEMSELVES AS SINGLETONS---
_cur_feats.execute("SELECT repID,repIDIndex FROM index2repID");

for repID,repIDIndex in _cur_feats:
    comb = str(repIDIndex);
    if not repID in non_singletons:
        rep_freq, bfdID = _cur_repre.execute("SELECT freq,id FROM representations WHERE repID=?",(repID,)).fetchall()[0];
    if not bfdID in minel_combs:
        minel_combs[bfdID] = Counter();
        bfdID2combs[bfdID] = set();
    minel_combs[bfdID][comb] += rep_freq;
    bfdID2combs[bfdID].add(comb);
#------------------------------------------------------------------------------------------------------------------------
print('Done preprocessing.');
#------------------------------------------------------------------------------------------------------------------------

TPs = 0;
Ts  = 0;
Rs  = 0;

for bfdID in minel_combs:
    index2comb = list(minel_combs[bfdID].keys());
    comb2index = {index2comb[i]:i for i in range(len(index2comb))};

    minel2combs = defaultdict(set);
    for comb in minel_combs[bfdID]:
        for minel in comb.split(','):
            minel2combs[minel].add(comb);

    comb2supersets = defaultdict(set);
    for comb in minel_combs[bfdID]:
        supersets            = set.intersection(*[minel2combs[minel] for minel in comb.split(',')]);
        comb2supersets[comb] = supersets;

    edges     = [(comb2index[superset],comb2index[comb]) for comb in comb2supersets for superset in comb2supersets[comb]];
    edges    += [(edge[1],edge[0]) for edge in edges]; # This cannot be used before transitive closure if the latter is applied!!!
    rows,cols = zip(*edges);
    size      = max(rows+cols)+1;
    reach     = csr((np.ones(len(edges),dtype=bool),(rows,cols,)),shape=(size,size,));
    max_combs = max((len(comb.split(',')) for comb in comb2index));
    #reach     = edge**max_combs; # This is not actually required as our edges already include all supersets, not just the most general
    freq      = np.array([minel_combs[bfdID][index2comb[i]] for i in range(len(index2comb))]);
    alle      = freq.sum();
    TP        = freq.dot(reach.dot(freq[:,None]))[0];
    T         = alle**2;
    TPs      += TP;
    Ts       += T;
    Rs       += TP/T;

    print('bfdID:',bfdID,'| combs:',len(index2comb),'| minels:',len(minel2combs),'| ments:',alle,'| rec:',int(100*TP/T),'| TP:',TP,'| T:',T,'| depth:',max_combs);
    print(','.join(minel2combs.keys()));

print('Overall micro average recall:',int(100*TPs/Ts));
print('Overall macro average recall:',int(100*Rs/len(minel_combs)));
