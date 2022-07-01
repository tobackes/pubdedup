import sys
import sqlite3
#import crossref_commons.retrieval as CR
from crossref.restful import Works, Etiquette
import random
import time
import multiprocessing as MP

_doi_db = sys.argv[1];

_num_workers = 16;
_batchsize   = 100;

_write_sleep       =   5;  #  5 seconds
_write_max_trytime = 1200; # 20 minutes

userinfo  = Etiquette('ProjectX', '1.0', None, 'nasibaer@live.de')

def get_type(doi,interface):
    t        = time.time();
    metadata = None;
    try:
        metadata = interface.doi(doi);#CR.get_publication_as_json(doi);
    except Exception as e:
        print(e);
        print('Problem accessing API for doi',doi);
        pass;
    response_time = time.time()-t; print(response_time,end='\r');
    typ           = metadata['type'] if metadata and 'type' in metadata else None;
    return (doi,typ,);

def work(Q,R,interface):
    while True:
        dois = get(Q);
        if dois != None:
            types = [get_type(doi,interface) for doi in dois];
            put(types,R);
        else:
            break;

def write(R,cur,con):
    while True:
        types = get(R,_write_sleep,_write_max_trytime);
        if types != None:
            cur.executemany("INSERT OR REPLACE INTO types VALUES(?,?)",types);
            con.commit();
            print('Written to disk.');
        else:
            break;

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

def start(workers,Q):
    con = sqlite3.connect(_doi_db);
    cur = con.cursor();
    cur.execute("SELECT doi FROM dois WHERE legal AND doi NOT IN (SELECT doi FROM types WHERE type IS NOT NULL)");
    while True:
        dois = [row[0] for row in cur.fetchmany(_batchsize)];
        if len(dois) == 0:
            break;
        put(dois,Q);
    con.close();
    for worker in workers:
        worker.start();

def join(workers):
    to_join = set(range(len(workers)));
    while len(to_join) > 0:
        i = random.sample(to_join,1)[0];
        workers[i].join(0.1);
        if not workers[i].is_alive():
            to_join.remove(i); print(len(to_join),'workers left to join.',end='\r');
        else:
            time.sleep(0.2);

def queue2list(Q):
    L = [];
    while True:
        element = get(Q);
        if element == None:
            break;
        L.append(element);
    return L;

def queue2generator(Q):
    L = [];
    while True:
        element = get(Q);
        if element == None:
            break;
        L.append(element);
    return L;

con     = sqlite3.connect(_doi_db);
cur     = con.cursor();
con_out = sqlite3.connect(_doi_db);
cur_out = con_out.cursor();

cur.execute("CREATE TABLE IF NOT EXISTS types(doi TEXT PRIMARY KEY, type TEXT)");

interfaces = [Works(etiquette=userinfo) for i in range(_num_workers)];
manager    = MP.Manager();
Q, R       = manager.Queue(), manager.Queue();

workers = [MP.Process(target=work,args=(Q,R,interfaces[x])) for x in range(_num_workers)];
writer  = MP.Process(target=write,args=(R,cur_out,con_out,));
start(workers,Q);
writer.start();
join(workers);
join([writer]);

#types = [pair for pairs in queue2generator(R) for pair in pairs]; #TODO: Would better be queue2generator(R) and then put that right into the executemany below...
#cur.executemany("INSERT OR REPLACE INTO types VALUES(?,?)",types);
#con.commit();
con.close();
con_out.close();
