import sqlite3
import json
import os,sys
import multiprocessing as MP
import time
import requests
import re
from collections import Counter
import itertools
import operator
from functools import reduce
from copy import deepcopy as copy

_indb      = sys.argv[1];
_outfolder = sys.argv[2];
_trafile   = sys.argv[3];
_termFreqs = sys.argv[4];
_nameFreqs = sys.argv[5];
_jobs      = int(sys.argv[6]);

_simple_freqs = 1.0;

_batchsize = 500000;
_dump      = 10000;
_wait      = 0.001;
_stopwords = ['the','a','of','with','to','where','by','as'];
_surpres   = set(['de','del','di','de la','von','van','della']);

WORD = re.compile(r'(\b[^\s]+\b)');

_transitions = dict();
MOD          = open(_trafile,'r');
for line in MOD:
    term1,term2,weight = line.rstrip().split(' ');
    if term1 in _transitions:
        _transitions[term1][term2] = float(weight);
    else:
        _transitions[term1] = {term2:float(weight)};
MOD.close();


def process(start,cur_in,cur_out,con_out):
    print("SELECT * FROM publications ORDER BY rowid LIMIT "+str(start)+","+str(_batchsize));
    cur_in.execute("SELECT * FROM publications ORDER BY rowid LIMIT ?,?",(start,_batchsize,));
    rows = []; i = 0;
    for row in cur_in:
        i   += 1;
        sims = similarize(row);
        for sim in sims:
            gens  = generalize(sim);
            rows += [sim] + gens;
        if i % _dump == 0:
            print(i);
            cur_out.executemany("INSERT INTO publications VALUES("+','.join(['?' for el in rows[-1]])+")",rows);
            con_out.commit();
            rows = [];
    print(i);
    if rows != []:
        cur_out.executemany("INSERT INTO publications VALUES("+','.join(['?' for el in rows[-1]])+")",rows);
    con_out.commit();

def is_valid(row):
    if row[6] == None and row[10] == None and row[14] == None and row[18] == None: # no surname
        return False;
    if row[22] == None and row[23] == None and row[24] == None and row[25] == None: # no term
        return False;
    return True;

def bundle(domain,row):
    groups = [[6,10,14,18],[7,11,15,19],[8,12,16,20],[9,13,17,21]] if domain=='name' else [[22,23,24,25]];
    row_   = copy(row);
    for group in groups:
        grouped = [row[i] for i in group if not row[i]==None];
        grouped = grouped + [None for x in range(len(group)-len(grouped))];
        for i in group:
            row_[i] = grouped[i];
    return row_;

def generalize(row):
    #mentionID,pubID,dupID,freq,title,year,a1surname,a1init,a1first,a1first_,a2surname,a2init,a2first,a2first_,a3surname,a3init,a3first,a3first_,a4surname,a4init,a4first,a4first_,term1,term2,term3,term4 = row;
    year_drops      = [set([5])];
    name_drops      = [set([6,7,8,9]),set([10,11,12,13]),set([14,15,16,17]),set([18,19,20,21])];
    term_drops      = [set([22]),set([23]),set([24]),set([25])];
    year_drops      = [set([drop for drop in year_drop if row[drop] != None]) for year_drop in year_drops];
    name_drops      = [set([drop for drop in name_drop if row[drop] != None]) for name_drop in name_drops];
    term_drops      = [set([drop for drop in term_drop if row[drop] != None]) for term_drop in term_drops];
    year_drops      = [year_drop for year_drop in year_drops if len(year_drop)>0];
    name_drops      = [name_drop for name_drop in name_drops if len(name_drop)>0];
    term_drops      = [term_drop for term_drop in term_drops if len(term_drop)>0];
    #generalizations = [[row[i] if (not i in name_drop and not i in term_drop) else None for i in range(len(row))] for name_drop in name_drops for term_drop in term_drops];
    yeneralizations = [[row[i] if not i in year_drop else None for i in range(len(row))] for year_drop in year_drops];
    neneralizations = [bundle('name',[row[i] if not i in name_drop else None for i in range(len(row))]) for name_drop in name_drops];
    teneralizations = [bundle('term',[row[i] if not i in term_drop else None for i in range(len(row))]) for term_drop in term_drops];
    generalizations = [generalization for generalization in yeneralizations+neneralizations+teneralizations if is_valid(generalization)];
    for i in range(len(generalizations)):
        generalizations[i][0] = generalizations[i][0]+'_'+str(i); # mentionID
        generalizations[i][3] = 0.0;                              # freq
        generalizations[i][4] = None;                             # title
    return generalizations;

def similarize(row):
    mentionID,pubID,dupID,freq,title,year,a1surname,a1init,a1first,a1first_,a2surname,a2init,a2first,a2first_,a3surname,a3init,a3first,a3first_,a4surname,a4init,a4first,a4first_,term1,term2,term3,term4 = row;
    represents   = [[(term,'title')] for term in [term1,term2,term3,term4] if term != None];
    represents   = [[term1,term2,term3,term4]]; #TODO: Could add more features like [,term4],[a1surname,a2...]
    optionss     = [[els for els in itertools.product(*[[(term2,_transitions[term1][term2],) for term2 in _transitions[term1]] if term1 in _transitions else [(term1,1.,)] for term1 in represents[i]])] for i in range(len(represents))];
    componentss  = [[part if not part==[] else None for part in option] for option in itertools.product(*[ [[term for term,weight in representation] for representation in component] for component in optionss])];
    weights      = [reduce(operator.mul,[weight for part in option for weight in part],1) for option in itertools.product(*[ [[weight for term,weight in representation] for representation in component] for component in optionss])]
    all_rows     = [tuple([mentionID+'_'+str(i),pubID,dupID,weights[i],title,year,a1surname,a1init,a1first,a1first_,a2surname,a2init,a2first,a2first_,a3surname,a3init,a3first,a3first_,a4surname,a4init,a4first,a4first_]+componentss[i][j]) for i in range(len(componentss)) for j in range(len(componentss[i]))]; #TODO: What if j is not always 0???
    return all_rows;

def work(Q,x):
    con_in  = sqlite3.connect(_indb);
    cur_in  = con_in.cursor();
    con_out = sqlite3.connect(_outfolder+str(x)+'.db');
    cur_out = con_out.cursor();
    cur_out.execute('DROP TABLE IF EXISTS publications');
    cur_out.execute('CREATE TABLE publications(mentionID TEXT PRIMARY KEY, wos_id TEXT, id TEXT, freq REAL, title TEXT, year INT, a1sur TEXT, a1init TEXT, a1first TEXT, a1firstonly TEXT, a2sur TEXT, a2init TEXT, a2first TEXT, a2firstonly TEXT, a3sur TEXT, a3init TEXT, a3first TEXT, a3firstonly TEXT, a4sur TEXT, a4init TEXT, a4first TEXT, a4firstonly TEXT, term1 TEXT, term2 TEXT, term3 TEXT, term4 TEXT)');
    while not Q.empty():
        print('Approximate number of jobs in queue:', Q.qsize());
        start = Q.get(timeout=60);
        process(start,cur_in,cur_out,con_out);
    con_in.close();
    con_out.close();

def feed(Q,indb):
    con = sqlite3.connect(indb);
    cur = con.cursor();
    num = cur.execute("SELECT count(*) FROM publications").fetchall()[0][0];
    for i in range(0,num,_batchsize):
        Q.put(i);
        while Q.qsize() > 10000:
            time.sleep(5);

#-----------------------------------------------------------------------------------------------------------------------
con_freq   = sqlite3.connect(_termFreqs);
cur_freq   = con_freq.cursor();
_freq_term = Counter({term.encode('utf-8'): freq for term,freq in cur_freq.execute("SELECT term,freq FROM terms") if freq!=None});
con_freq.close();
#-----------------------------------------------------------------------------------------------------------------------
con_freq  = sqlite3.connect(_nameFreqs);
cur_freq  = con_freq.cursor();
_freq_sur = Counter({name.encode('utf-8'): freq for name,freq in cur_freq.execute("SELECT name,freq FROM surnames   WHERE name IS NOT NULL")});
_freq_1st = Counter({name.encode('utf-8'): freq for name,freq in cur_freq.execute("SELECT name,freq FROM firstnames WHERE name IS NOT NULL")});
con_freq.close();
#-----------------------------------------------------------------------------------------------------------------------

Q = MP.Queue();

feeder  = MP.Process(target=feed,args=(Q,_indb,));
workers = [MP.Process(target=work,args=(Q,x,)) for x in range(_jobs)];

feeder.start(); time.sleep(1);

for worker in workers:
    worker.start();

for worker in workers:
    worker.join();
#-----------------------------------------------------------------------------------------------------------------------
