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
from nltk.corpus import stopwords

_infolder  = sys.argv[1];
_outfolder = sys.argv[2];
_modfile   = sys.argv[3];
_termFreqs = sys.argv[4];
_nameFreqs = sys.argv[5];
_jobs      = int(sys.argv[6]);

_simple_freqs = 1.0;

_dump      = 10000;
_wait      = 0.001;


_stopwords = set().union(*[set(stopwords.words(lang)) for lang in ['english','german','french','italian','spanish','russian','portuguese','dutch','swedish','danish','finnish']]);
_surpres   = set(['de','del','di','de la','von','van','della']);

WORD  = re.compile(r'(\b[^\s]+\b)'); #TODO: Make stricter
STRIP = re.compile(r'(^(\s|,)+)|((\s|,)+$)');

MOD          = open(_modfile,'r');
_transforms  = {line.rstrip().split(' ')[0]:line.rstrip().split(' ')[1] for line in MOD};
MOD.close();


def process(filename,cur,con):
    IN   = open(_infolder+filename);
    rows = [];
    i    = 0;
    for line in IN:
        i += 1;
        entry = json.loads(line);
        rows.append(parse(entry));
        if i % _dump == 0:
            print(i);
            cur.executemany("INSERT INTO mentions VALUES("+','.join(['?' for el in rows[-1]])+")",rows);
            con.commit();
            rows = [];
    print(i);
    if rows != []:
        cur.executemany("INSERT INTO mentions VALUES("+','.join(['?' for el in rows[-1]])+")",rows);
    con.commit();
    IN.close();

def parse(entry):
    mentionID       = entry['coreId'];
    pubID           = entry['coreId'];
    dupID           = entry['doi'];
    title           = entry['title'];
    title_          = ' '.join([term.lower() for term in title.split()]) if title != None else None;
    date            = entry['datePublished'];
    year            = get_year(date);
    year1           = int(str(year-1)+str(year)) if year != None else None;
    year2           = int(str(year)+str(year+1)) if year != None else None;
    authors         = get_authors(entry['authors']);
    a1surname       = authors[0]['surname']             if len(authors)>0 else None;
    a1init          = a1surname+'_'+authors[0]['init']  if len(authors)>0 and a1surname!=None and a1surname != None and authors[0]['init'] !=None else None;
    a1first         = a1surname+'_'+authors[0]['first'] if len(authors)>0 and a1surname!=None and a1surname != None and authors[0]['first']!=None else None;
    a2surname       = authors[1]['surname']             if len(authors)>1 else None;
    a2init          = a2surname+'_'+authors[1]['init']  if len(authors)>1 and a1surname!=None and a2surname != None and authors[1]['init'] !=None else None;
    a2first         = a2surname+'_'+authors[1]['first'] if len(authors)>1 and a1surname!=None and a2surname != None and authors[1]['first']!=None else None;
    a3surname       = authors[2]['surname']             if len(authors)>2 else None;
    a3init          = a3surname+'_'+authors[2]['init']  if len(authors)>2 and a1surname!=None and a3surname != None and authors[2]['init'] !=None else None;
    a3first         = a3surname+'_'+authors[2]['first'] if len(authors)>2 and a1surname!=None and a3surname != None and authors[2]['first']!=None else None;
    a4surname       = authors[3]['surname']             if len(authors)>3 else None;
    a4init          = a4surname+'_'+authors[3]['init']  if len(authors)>3 and a1surname!=None and a4surname != None and authors[3]['init'] !=None else None;
    a4first         = a4surname+'_'+authors[3]['first'] if len(authors)>3 and a1surname!=None and a4surname != None and authors[3]['first']!=None else None;
    terms           = get_terms(title);
    term1, term1gen = terms[0] if len(terms)>0 else (None,None);
    term2, term2gen = terms[1] if len(terms)>1 else (None,None);
    term3, term3gen = terms[2] if len(terms)>2 else (None,None);
    term4, term4gen = terms[3] if len(terms)>3 else (None,None);
    return (mentionID,pubID,dupID,1.0,title_,year1,year2,a1surname,a1init,a1first,a2surname,a2init,a2first,a3surname,a3init,a3first,a4surname,a4init,a4first,term1,term2,term3,term4,term1gen,term2gen,term3gen,term4gen);

def get_year(date):
    if date == None:
        return None;
    parts = date.split('-');
    first = parts[0] if len(parts)>0 else None;
    year  = None;
    if len(first)==4:
        try:
            year = int(first);
        except:
            print('Cannot take integer of', first);
    return year;

def get_authors(authorlist): #TODO: There are still surnames and firstnames with only one letter!
    authors = [];
    for authorname in authorlist:
        authorname = re.sub(STRIP,'',authorname); #remove beginning or ending whitespace or comma
        parts      = authorname.split(',') if ',' in authorname else [authorname.split(' ')[-1],' '.join(authorname.split(' ')[:-1])];
        parts      = [part.strip() for part in parts];
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
            print('surname only one letter and firstname more frequent as surname:');
            print('surname:', surname, '--- firstname:', firstname, '--- firstinit:', firstinit);
        if surname != None and firstname != None and len(firstname)>1 and len(surname)>1 and _freq_sur[surname] < _freq_1st[surname] and _freq_1st[firstname] < _freq_sur[firstname]:
            temp      = surname;
            surname   = firstname;
            firstname = temp;
            firstinit = firstname[0];
            print('surname more frequent as firstname and firstname more frequent as surname:');
            print('surname:', surname, '--- firstname:', firstname, '--- firstinit:', firstinit);
            print(surname, _freq_sur[surname], firstname, _freq_1st[firstname]);
        if surname != None and '.' in surname:
            firstinit = surname.replace('.','')[0] if len(surname.replace('.',''))>0 else None;
            temp      = firstname;
            firstname = surname.replace('.','') if len(surname.replace('.','')) > 1 else None;
            surname   = temp;
        authors.append({'string':authorname,'surname':surname, 'first':firstname, 'init':firstinit});
    return authors;

def get_terms(title):
    if title == None:
        return [];
    terms = [term.lower() for term in re.findall(WORD,title)];#[_transform[term] if term in _transform else term for term in [term.lower() for term in re.split(r'(\W)+']];
    terms = sorted([(_freq_term[term],term,) for term in terms if not term in _stopwords and len(term)>2]);
    return [(term,_transforms[term],) if term in _transforms else (term,term,) for freq,term in terms];

def work(Q,x):
    con      = sqlite3.connect(_outfolder+str(x)+'.db');
    cur      = con.cursor();
    cur.execute('DROP TABLE IF EXISTS mentions');
    cur.execute('CREATE TABLE mentions(mentionID INT, originalID TEXT, goldID TEXT, freq REAL, title TEXT, year1 INT, year2 INT, a1sur TEXT, a1init TEXT, a1first TEXT, a2sur TEXT, a2init TEXT, a2first TEXT, a3sur TEXT, a3init TEXT, a3first TEXT, a4sur TEXT, a4init TEXT, a4first TEXT, term1 TEXT, term2 TEXT, term3 TEXT, term4 TEXT, term1gen TEXT, term2gen TEXT, term3gen TEXT, term4gen TEXT)');
    while not Q.empty():
        print('Approximate number of jobs in queue:', Q.qsize());
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

feeder  = MP.Process(target=feed,args=(Q,_infolder,));
workers = [MP.Process(target=work,args=(Q,x,)) for x in range(_jobs)];

feeder.start(); time.sleep(1);

for worker in workers:
    worker.start();

for worker in workers:
    worker.join();
#-----------------------------------------------------------------------------------------------------------------------
