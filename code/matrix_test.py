import numpy as np
from scipy.sparse import csr_matrix as csr

num_pub = 10000000;
num_els = 1000000;
num_val = num_pub * 30;

rows = np.random.randint(0,num_pub,num_val);
cols = np.random.randint(0,num_els,num_val);
data = np.ones(num_val,dtype=bool);

c = csr((data,(rows,cols)),shape=(num_pub,num_els),dtype=bool);

set_sizes = c.sum(1);
max_sizes = np.max(set_sizes);
size_sets = csr((np.ones(num_pub,dtype=bool),(np.arange(max_sizes),set_sizes)),shape=(max_sizes,num_pub));

def el2sets(el):
    return c[:,el].nonzero()[0];

def size2sets(size):
    return c[size,:].nonzero()[0];

if True:
    parent     = np.arange(num_pub);
    number     = np.ones(num_pub,dtype=int);
    unassigned = size_sets.copy();
    for size in xrange(unassigned.shape[0]):
        sizels = unassigned[size].nonzero()[1];
        for i in sizels:
            #TODO: Continue here...
            specifications = set().union(*[set.intersection(*[el2sets[size_][el] for el in reps[i] if el in el2sets[size_]]) if not reps[i].isdisjoint(set(el2sets[size_].keys())) else [] for size_ in el2sets if size_ > size]);
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
                    number[to] += 1;
                unassigned[len(reps[j])] -= set([j]);
    #------------------------------------ COMPRESS(parent)
    for i in xrange(len(parent)):
        par,z = None,i;
        while True:
            par = parent[z];
            if par == z: break;
            z = par;
        parent[i] = par;
