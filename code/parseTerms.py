import sqlite3
import json
import os,sys
import multiprocessing as MP
import time
import requests
import re
from collections import Counter

_infolder  = sys.argv[1];
#_freqDB    = sys.argv[2];
_outfolder = sys.argv[2];
_jobs      = int(sys.argv[3]);

_dump      = 10000;
_wait      = 0.001;

_num_fields = 3;

_stopwords = ['the','a','of','with','to','where','by','as'];


def process(filename,cur,con):
    IN   = open(_infolder+filename);
    rows = [];
    i    = 0;
    for line in IN:
        i += 1;
        entry = json.loads(line);
        rows_ = parse(entry);
        rows  += rows_;
        if i % _dump == 0:
            print i;
            cur.executemany("INSERT INTO terms VALUES("+','.join(['?' for x in xrange(_num_fields)])+")",rows);
            con.commit();
            rows = [];
    print i;
    if rows != []:
        cur.executemany("INSERT INTO terms VALUES("+','.join(['?' for x in xrange(_num_fields)])+")",rows);
    con.commit();
    IN.close();

def parse(entry):
    pubID   = entry['coreId'];
    terms   = get_terms(entry['title']);
    return [(pubID+'_'+str(i),pubID,terms[i]) for i in xrange(len(terms))];

def get_terms(title): #TODO: Improve by better splitting, normalization and ranking
    if title == None:
        return [];
    terms = [term.lower() for term in re.split(r'(\W)+',title) if not term.lower() in _stopwords and len(term)>2];
    return terms;

def work(Q,x):
    con      = sqlite3.connect(_outfolder+str(x)+'.db');
    cur      = con.cursor();
    cur.execute('DROP TABLE IF EXISTS terms');
    cur.execute('CREATE TABLE terms(mentionID TEXT PRIMARY KEY, pubID TEXT, term TEXT)');
    while not Q.empty():
        print 'Approximate number of jobs in queue:', Q.qsize();
        filename = Q.get(timeout=60);
        process(filename,cur,con);
    con.close();

def feed(Q,infolder):
    for filename in os.listdir(infolder):
        if not filename.endswith('.json'):
            continue;
        Q.put(filename);
        while Q.qsize() > 10000:
            time.sleep(5);

#-----------------------------------------------------------------------------------------------------------------------
Q = MP.Queue();

feeder  = MP.Process(target=feed,args=(Q,_infolder,));
workers = [MP.Process(target=work,args=(Q,x,)) for x in xrange(_jobs)];

feeder.start(); time.sleep(1);

for worker in workers:
    worker.start();

for worker in workers:
    worker.join();
#-----------------------------------------------------------------------------------------------------------------------
