import sqlite3
import json
import os,sys
import multiprocessing as MP
import time
import requests
import re
from collections import Counter

_infolder  = sys.argv[1];
_freqDB    = sys.argv[2];
_outfolder = sys.argv[3];
_jobs      = int(sys.argv[4]);

_surpres = set(['de','del','di','de la','von','van','della']);

_dump      = 10000;
_wait      = 0.001;

_num_fields = 6;

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
            cur.executemany("INSERT INTO names VALUES("+','.join(['?' for x in xrange(_num_fields)])+")",rows);
            con.commit();
            rows = [];
    print i;
    if rows != []:
        cur.executemany("INSERT INTO names VALUES("+','.join(['?' for x in xrange(_num_fields)])+")",rows);
    con.commit();
    IN.close();

def parse(entry):
    pubID   = entry['coreId'];
    authors = get_authors(entry['authors']);
    return [(pubID+'_'+str(i),pubID,authors[i]['string'],authors[i]['surname'],authors[i]['first'],authors[i]['init']) for i in xrange(len(authors))];

def get_authors(authorlist): #TODO: There are still surnames and firstnames with only one letter!
    authors = [];
    for authorname in authorlist:
        parts      = authorname.split(',') if ',' in authorname else [authorname.split(' ')[-1],' '.join(authorname.split(' ')[:-1])];
        surname    = parts[0].lower();
        firstnames = [el for el in parts[1].replace('.',' ').split(' ') if not el==''] if len(parts)>1 else [];
        firstname  = firstnames[0].lower() if len(firstnames)>0 and len(firstnames[0]) > 1 else None;
        firstinit  = firstnames[0][0].lower() if len(firstnames)>0 and firstnames[0] != '' else None;
        if firstname != None and '-' in firstname:
            subfirsts = [el for el in firstname.split('-') if len(el)>0];
            if len(subfirsts) == 0:
                firstname = None;
            elif len(subfirsts[0]) == 1:
                firstinit = subfirsts[0];
                firstname = None;
        if firstname in _surpres:
            surnames = [];
            i        = 0;
            while len(firstnames[i]) > 1 and i < len(firstnames)-1:
                surnames.append(firstnames[i].lower());
                i += 1;
            firstnames = firstnames[i:];
            firstname  = firstnames[0].lower() if len(firstnames)>0 and len(firstnames[0]) > 1 else None;
            firstinit  = firstname[0] if firstname != None else None;
            surname    = ' '.join(surnames);
        if surname != None and firstname != None and len(surname) == 1:# and _freq_1st[firstname] < _freq_sur[firstname]:
            temp      = surname;
            surname   = firstname;
            firstname = temp;
            firstinit = firstname[0];
            print 'surname only one letter and firstname more frequent as surname:';
            print 'surname:', surname, '--- firstname:', firstname, '--- firstinit:', firstinit;
        if surname != None and firstname != None and len(firstname)>1 and len(surname)>1 and _freq_sur[surname] < _freq_1st[surname] and _freq_1st[firstname] < _freq_sur[firstname]:
            temp      = surname;
            surname   = firstname;
            firstname = temp;
            firstinit = firstname[0];
            print 'surname more frequent as firstname and firstname more frequent as surname:';
            print 'surname:', surname, '--- firstname:', firstname, '--- firstinit:', firstinit;
            print surname, _freq_sur[surname], firstname, _freq_1st[firstname];
        if surname != None and '.' in surname:
            firstinit = surname.replace('.','')[0] if len(surname.replace('.',''))>0 else None;
            temp      = firstname;
            firstname = surname.replace('.','') if len(surname.replace('.','')) > 1 else None;
            surname   = temp;
        authors.append({'string':authorname,'surname':surname, 'first':firstname, 'init':firstinit});
    return authors;

def work(Q,x):
    con      = sqlite3.connect(_outfolder+str(x)+'.db');
    cur      = con.cursor();
    cur.execute('DROP TABLE IF EXISTS names');
    cur.execute('CREATE TABLE names(mentionID TEXT PRIMARY KEY, pubID TEXT, string TEXT, surname TEXT, firstname TEXT, firstinit TEXT)');
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
#-----------------------------------------------------------------------------------------------------------------------
con_freq = sqlite3.connect(_freqDB);
cur_freq = con_freq.cursor();

_freq_sur  = Counter({name.encode('utf-8'): freq for name,freq in cur_freq.execute("SELECT name,freq FROM surnames   WHERE name IS NOT NULL")});
_freq_1st  = Counter({name.encode('utf-8'): freq for name,freq in cur_freq.execute("SELECT name,freq FROM firstnames WHERE name IS NOT NULL")});

con_freq.close();
#-----------------------------------------------------------------------------------------------------------------------
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
