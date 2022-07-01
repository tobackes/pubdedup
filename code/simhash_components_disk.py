#-IMPORTS-------------------------------------------------------------------------------------------------------------------------------------------
import sqlite3
import sys
import os, psutil
import numpy as np
from collections import defaultdict, Counter
import time
import random
import multiprocessing as MP
import multiprocessing.pool
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components
from scipy.sparse import vstack as csr_vstack
from scipy.sparse import hstack as csr_hstack
from copy import deepcopy as copy
#---------------------------------------------------------------------------------------------------------------------------------------------------
#-GLOBALS-------------------------------------------------------------------------------------------------------------------------------------------

_inDB         = sys.argv[1];
_reportDB     = sys.argv[2]; # Not used
_componentsDB = sys.argv[3];

#_workers   = 8;
_batchsize = 2500000;
_patchsize = 1;

_num_workers   = 64;
_tasks_per_job = 2;

#---------------------------------------------------------------------------------------------------------------------------------------------------
#-FUNCTIONS-----------------------------------------------------------------------------------------------------------------------------------------

def put(value,queue,sleeptime=0.1,max_trytime=1):
    start_time = time.time();
    try_time   = 0;
    while True:
        try:
            queue.put(value,block=False);
            break;
        except Exception as e:
            try_time = time.time() - start_time;
            if try_time > max_trytime:
                return 1;
            time.sleep(sleeptime);

def get(queue,sleeptime=0.02,max_trytime=0.1):
    start_time = time.time();
    try_time   = 0;
    value      = None;
    while True:
        try:
            value = queue.get(block=False);
            break;
        except Exception as e:
            try_time = time.time() - start_time;
            if try_time > max_trytime:
                break;
            time.sleep(sleeptime);
    return value;

def queue2list(Q):
    L = [];
    while True:
        element = get(Q);
        if element == None:
            break;
        L.append(element);
    return L;

def join(workers):
    to_join = set(range(len(workers)));
    print(len(to_join),'workers left to join.',end='\r');
    while len(to_join) > 0:
        i = random.sample(to_join,1)[0];
        workers[i].join(0.1);
        if not workers[i].is_alive():
            to_join.remove(i); print(len(to_join),'workers left to join.',end='\r');
        else:
            time.sleep(0.2);

def start(workers,jobs,Q):
    for job in jobs:
        put(job,Q);
    for worker in workers:
        worker.start();

def get_featsOf(cur,start=None,end=None):
    if start == None:
        print('start==None.')
        cur.execute("SELECT * FROM features");
        rows = sorted([(int(repIDIndex),int(featIndex),) for repIDIndex,featIndex in cur]);
    else:
        cur.execute("SELECT * FROM features WHERE repIDIndex BETWEEN ? AND ? ORDER BY repIDIndex",(start,end,));
    first   = cur.fetchone();
    featsOf = [[first[1]]];
    current = first[0];
    for repIDIndex, featIndex in cur:
        if repIDIndex != current:
            current     = repIDIndex;
            featsOf[-1] = np.array(featsOf[-1]);
            featsOf.append([]);
        featsOf[-1].append(featIndex);
    featsOf[-1] = np.array(featsOf[-1]);
    featsOf     = np.array(featsOf,dtype=object);
    return featsOf;

def make_matrix(featlists,d):
    rows = [featIndex for featlist in featlists             for featIndex in featlist];
    cols = [i         for i        in range(len(featlists)) for j         in range(len(featlists[i]))];
    data = np.ones(len(rows),dtype=bool);
    D    = csr((data,(rows,cols)),dtype=bool,shape=(d,len(featlists),));
    return D;

def randomvector(d,ignore):
    components = np.random.standard_normal(d);
    r          = np.sqrt(np.square(components).sum());
    v          = components / r;
    v[ignore]  = 0;
    v          = csr(v,shape=(1,d));
    return v;

def bits2int(bits):
  m,n = bits.shape;
  a   = 2**np.arange(n,dtype=np.uint64)[::-1];
  return bits @ a;

def make_tasks(batchsize,patchsize,n):
    _p      = 50;
    patches = [(i,min(i+patchsize  ,_p  ),) for i in range(0,_p,patchsize)]; # simple range excluding the last integer
    batches = [(i,min(i+batchsize-1, n  ),) for i in range(0, n,batchsize)]; # sqlite range including the last integer
    tasks   = [(batch,patches,) for batch in batches];
    return tasks;

def get_hashes(tasks,d,cur): #Assuming that sizes is smallest first! #TODO: cur needs to to be different cursors
    col = 0;
    for batch,patches in tasks:
        featlists = get_featsOf(cur,batch[0],batch[1]);
        D         = make_matrix(featlists,d);
        for start,end in patches:
            H = None;
            for i in range(start,end):
                h = randomvector(d,[]);
                if H is None:
                    H = h.copy();
                else:
                    H = csr_vstack([H,h]);
            yield col, H.dot(D).toarray()>0;
        col += 1;

def work(Q,R,cur):
    tasks = get(Q);
    while tasks != None:
        first_repIDIndex = tasks[0][0][0]; # First task -> batch -> start
        #-------------------------------------------------------------------------------------------------------------------------------------
        labels       = [];
        hash_col     = None;
        hash_col_num = -1;
        last_col     = -1;
        t            = time.time();
        t_           = time.time();
        for hash_col_num,hash_row_vals in get_hashes(tasks,d,cur):
            print(round((time.time()-t)/_patchsize,2),'s x',_patchsize,end='\r'); t = time.time();
            if last_col != hash_col_num: # Finished a batchsize x p matrix with batchsize hashes -- or beginning
                if last_col >= 0: # Except in the beginning, hstack the previous column vectors to the global hashes matrix
                    labels += list(bits2int(hash_col.T));
                    print(hash_row_vals.shape,hash_col.shape,len(labels),labels[-1]); print(round((time.time()-t_),2),'s'); t_ = time.time();
                last_col = hash_col_num;
                hash_col = hash_row_vals; # Beginning the new columns
            else:
                hash_col = np.vstack([hash_col,hash_row_vals]); # Continuing the current column
        labels += list(bits2int(hash_col.T));
        #-------------------------------------------------------------------------------------------------------------------------------------
        repIDIndex2label = {first_repIDIndex+i:labels[i] for i in range(len(labels))};
        R.put(repIDIndex2label);
        tasks = get(Q);

def get_feats(repIDIndex,cur):
    return cur.execute('SELECT featGroup||":"||feat FROM index2feat WHERE featIndex IN (SELECT featIndex FROM features WHERE repIDIndex='+str(repIDIndex)+')').fetchall();

#-SCRIPT--------------------------------------------------------------------------------------------------------------------------------------------

#-PREPARE INPUT AND OUTPUT--------------------------------------------------------------------------------------------------------
con = sqlite3.connect(_inDB);
cur = con.cursor();
#---------------------------------------------------------------------------------------------------------------------------------

#-MAKE THE TASK BATCHES AND PATCHES-----------------------------------------------------------------------------------------------
n     = cur.execute("SELECT MAX(repIDIndex) FROM index2repID").fetchall()[0][0]+1; # Since 0 is an index as well, we have to add one
d     = cur.execute("SELECT MAX(featIndex)  FROM index2feat" ).fetchall()[0][0]+1; # Since 0 is an index as well, we have to add one
tasks = make_tasks(_batchsize,_patchsize,n);
jobs  = [tasks[i:min(i+_tasks_per_job,len(tasks))] for i in range(0,len(tasks),_tasks_per_job)];
#---------------------------------------------------------------------------------------------------------------------------------

#-COMPUTING THE SIMHASHES---------------------------------------------------------------------------------------------------------
print(n,'representations to hash...', len(jobs),'jobs to work...');

cons = [sqlite3.connect(_inDB) for x in range(_num_workers)];
curs = [cons[x].cursor()       for x in range(_num_workers)];

manager = MP.Manager();
Q, R    = manager.Queue(), manager.Queue();
workers = [MP.Process(target=work,args=(Q,R,curs[x],)) for x in range(_num_workers)];
start(workers,jobs,Q);
time.sleep(60);
join(workers);

for i in range(len(cons)):
    cons[i].close();
    print(i,'connections left to close.',end='\r');
con.close();

print('Getting results from queue...');
repIDIndex2label_dicts = queue2list(R);
print('Done getting results from queue.');

print('Combining results into component mapping...'); #TODO: The whole dictionary structure is quite memory inefficient. It requires 100GB of RAM, while a numpy array would be much smaller.
index2label = [];
label2index = dict();
components  = dict();
for repIDIndex2label in repIDIndex2label_dicts:
    for repIDIndex in repIDIndex2label:
        label = repIDIndex2label[repIDIndex];
        if not label in label2index:
            label2index[label] = len(index2label);
            index2label.append(label);
        labelIndex = label2index[label];
        if labelIndex in components:
            components[labelIndex].add(repIDIndex);
        else:
            components[labelIndex] = set([repIDIndex]);
components = [components[labelIndex] for labelIndex in range(len(components))];
print('Done combining results into component mapping.');

#TODO: For each repIDIndex, we now have a label
#      We need to relate the labels so that the ones that are <=3 different are related
#      We can use the set of all labels without repetition
#      Then we can apply the relations and the closure to set the labels equal somehow
#      Alternatively as there are few equal labels anyway, we can also use as N the labelling as such and relabel

#index2label = list(set(labels));
#label2index = {index2label[i]:i for i in range(len(index2label))};
#for i in range(len(labels)):
#   labelIndex = label2index[labels[i]];
#   if labelIndex in components:
#       components[labelIndex].add(i);
#   else:
#       components[labelIndex] = set([i]);

#for labelIndex in range(len(components)):
#    if len(components[labelIndex]) >= 3:
#        print('--------------------------------------');
#        for repIDIndex in components[labelIndex]:
#            print(get_feats(repIDIndex,cur));

#-OUTPUT THE RESULTS--------------------------------------------------------------------------------------------------------------
print('Writing out...');
con_out = sqlite3.connect(_componentsDB);
cur_out = con_out.cursor();
cur_out.execute("DROP TABLE IF EXISTS components");
cur_out.execute("DROP INDEX IF EXISTS label_index");
cur_out.execute("CREATE TABLE components(label INT, repIDIndex INT)");
cur_out.executemany("INSERT INTO components VALUES(?,?)",((label,repIndex,) for label in range(len(components)) for repIndex in components[label] if len(components[label]) > 0)); #TODO: Can be changed
cur_out.execute("CREATE INDEX label_index on components(label)");
cur_out.execute("CREATE INDEX repIDIndex_index on components(repIDIndex)");
con_out.commit();
con_out.close();
print('Done writing out.');
