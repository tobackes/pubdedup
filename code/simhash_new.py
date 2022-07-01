import numpy as np
import gmpy2
import itertools
import time
from scipy.sparse import csr_matrix as csr
import multiprocessing as MP
from functools import partial


_k          = 3;

#for x in range(64):
#    N.sort();
#    for i in range(_windowsize):
#        similars = np.array([gmpy2.popcount(int(el)) for el in N^np.roll(N,i)])<=3;
#    N >> 1; #The last change will turn the numbers back to original and won't be checked

# We split the 64bit hash into 8 x 8 bits and we want at most k=3 differences
# Then 5 out of 8 blocks must be identical, that is the first 5 blocks must be identical for at least one table
# We draw 5 blocks from 8 without replacement and without regarding their order, which is 8 over 5 = 56 combinations (tables)
# For each sorted table we go linearly through the hashes and compare the preceeding 5 integers, we store the sorting
# This creates a labelling representing necessary condition for this table, but not sufficient
# We have to check the pairs with the same label for their remaining 3 blocks having only 3 variations

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

def transitive_closure(M):
    edges     = set_diagonal(M,csr(np.zeros(M.shape[0],dtype=bool)[:,None]));
    closure   = edges.copy();
    num, i    = 1,2;
    while num > 0:
        #print('...',i,':',num);
        new        = edges**i;
        num        = len(new.nonzero()[0]);
        closure    = closure + new;
        i         += 1;
        closure.eliminate_zeros();
        if closure.diagonal().sum() > 0:
            print('WARNING: Cycles in input matrix!');
    return set_diagonal(closure,csr(M.diagonal()[:,None])).astype(bool);

def get_closure(tups):
    index2id = list(set([tup[0] for tup in tups]) | set([tup[1] for tup in tups]));
    id2index = {index2id[i]:i for i in range(len(index2id))};
    tups_re  = tups #+ [(index2id[i],index2id[i],) for i in range(len(index2id))]; # Unfortunately you have to make the relation reflexive first - you could also add the diagonal to M
    M        = csr( ([True for tup in tups_re],([id2index[tup[0]] for tup in tups_re],[id2index[tup[1]] for tup in tups_re])),shape=(len(index2id),len(index2id)),dtype=bool);
    M_       = transitive_closure(M); # n is maximum path length of your relation
    temp     = M_.nonzero();
    return [(index2id[temp[0][i]],index2id[temp[1][i]],) for i in range(len(temp[0]))];

def make_blockints_(bitstrings):
    integerss  = np.array([split_interpret(bitstring) for bitstring in bitstrings],dtype=np.uint8);
    return integerss;

def split_interpret(bitstring): # assume 64 / 8
    parts = [bitstring[i:i+8] for i in range(0,64,8)];
    ints  = [int(part,2) for part in parts];
    return ints;

def make_blockints(integers):
    integerss  = np.array([split_interpret(make_bitstring(integer)) for integer in integers],dtype=np.uint8);
    return integerss;

def make_bitstrings(integers):
    bitstrings = np.vectorize(np.binary_repr)(integers,64);
    return bitstrings;

def make_bitstring(integer):
    bitstring = np.binary_repr(integer,64);
    return bitstring;

def simhash(ordering,T,N,k):
    times = []; t = time.process_time();
    #print('-----------------------------');
    T_        = T[:,ordering]; times.append(time.process_time()-t); t = time.process_time();
    sorting   = np.lexsort(T_[:,::-1].T[:8-k]); times.append(time.process_time()-t); t = time.process_time(); # This takes the longest, around 1 sec for 1M rows
    T__       = T[sorting,:]; times.append(time.process_time()-t); t = time.process_time();
    equals    = np.where((T__[:-1,:8-k] == T__[1:,:8-k]).sum(axis=1)==8-k)[0]; times.append(time.process_time()-t); t = time.process_time();
    equivs    = [(sorting[index],sorting[index+1],) for index in equals]; times.append(time.process_time()-t); t = time.process_time(); # Tuples of indices in the unsorted table which have identical prefix for this ordering
    equivs    = get_closure(equivs); times.append(time.process_time()-t); t = time.process_time();
    #print(len(equivs),'satisfying necessary condition for this table');
    checked   = [(i1,i2,) for i1,i2 in equivs if gmpy2.popcount(int(N[i1]^N[i2])) <= k]; times.append(time.process_time()-t); t = time.process_time();
    #print(checked); print(times);
    return set(checked);

def find_similar(N,k):
    orderings = itertools.combinations(range(8),8-k);
    orderings = [list(ordering)+[index for index in range(8) if not index in ordering] for ordering in orderings];
    T         = make_blockints(N);
    with MP.Pool(56) as pool:
        similarss = pool.map(partial(simhash,T=T,N=N,k=k),orderings);
    print(len(similarss));
    similars  = set().union(*similarss);
    return similars;

def do_label(similars):
    labelling = list(range(len(N)));
    for i,j in sorted([(i,j) if i<j else (j,i) for i,j in similars]):
        labelling[j] = labelling[i];
    labelling = np.array(labelling,dtype=int);
    #TODO: Make compact like in AD code
    return labelling;

N        = np.random.randint(18446744073709551615,size=1000000000,dtype=np.uint64);

t        = time.time();
similars = find_similar(N,_k);
print(time.time()-t);
