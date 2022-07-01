import sqlite3
import json
import os,sys
import multiprocessing as MP
import time
import requests
import re

_infolder  = sys.argv[1];
_outfolder = sys.argv[2];
_modfile   = sys.argv[3];
_trafile   = sys.argv[4];
_termFreqs = sys.argv[5];
_nameFreqs = sys.argv[6];
_jobs      = int(sys.argv[7]);

_simple_freqs = 1.0;

_dump      = 10000;
_wait      = 0.001;
_stopwords = ['the','a','of','with','to','where','by','as'];

WORD = re.compile(r'(\b[^\s]+\b)');

_transitions = dict();
MOD          = open(_trafile'r');
for line in MOD:
    for term1,term2,weight in line.rstrip().split(' '):
        if term1 in _transitions:
            _transitions[term1][term2] = float(weight);
        else:
            _transitions[term1] = {term2:float(weight)};
MOD.close();

MOD          = open(_modfile,'r');
_transforms  = {line.rstrip().split(' ')[0]:line.rstrip().split(' ')[1] for line in MOD};
MOD.close();


def process(filename,cur,con,cur_freq,con_freq):
    IN   = open(_infolder+filename);
    rows = [];
    i    = 0;
    for line in IN:
        i += 1;
        entry = json.loads(line);
        row   = parse(entry,cur_freq,con_freq);
        sims  = similarize(row);
        for sim in sims:
            gens  = generalize(sim);
            rows  += [sim] + gens;
        if i % _dump == 0:
            print i;
            cur.executemany("INSERT INTO publications VALUES("+','.join(['?' for el in rows[-1]])+")",rows);
            con.commit();
            rows = [];
    print i;
    if rows != []:
        cur.executemany("INSERT INTO publications VALUES("+','.join(['?' for el in rows[-1]])+")",rows);
    con.commit();
    IN.close();

def parse(entry,cur_freq,con_freq):
    mentionID = entry['coreId'];
    pubID     = entry['coreId'];
    dupID     = entry['doi'];
    title     = entry['title'];
    title_    = ' '.join([term.lower() for term in title.split()]);
    date      = entry['datePublished'];
    year      = get_year(date);
    authors   = get_authors(entry['authors']);
    a1surname = authors[0]['surname']             if len(authors)>0 else None;
    a1init    = a1surname+'_'+authors[0]['init']  if len(authors)>0 and a1surname!=None and authors[0]['init'] !=None else None;
    a1first   = a1surname+'_'+authors[0]['first'] if len(authors)>0 and a1surname!=None and authors[0]['first']!=None else None;
    a1first_  = authors[0]['first'] if len(authors)>0 else None;
    a2surname = authors[1]['surname']             if len(authors)>1 else None;
    a2init    = a2surname+'_'+authors[1]['init']  if len(authors)>1 and a1surname!=None and authors[1]['init'] !=None else None;
    a2first   = a2surname+'_'+authors[1]['first'] if len(authors)>1 and a1surname!=None and authors[1]['first']!=None else None;
    a2first_  = authors[1]['first'] if len(authors)>1 else None;
    a3surname = authors[2]['surname']             if len(authors)>2 else None;
    a3init    = a3surname+'_'+authors[2]['init']  if len(authors)>2 and a1surname!=None and authors[2]['init'] !=None else None;
    a3first   = a3surname+'_'+authors[2]['first'] if len(authors)>2 and a1surname!=None and authors[2]['first']!=None else None;
    a3first_  = authors[2]['first'] if len(authors)>2 else None;
    a4surname = authors[3]['surname']             if len(authors)>3 else None;
    a4init    = a4surname+'_'+authors[3]['init']  if len(authors)>3 and a1surname!=None and authors[3]['init'] !=None else None;
    a4first   = a4surname+'_'+authors[3]['first'] if len(authors)>3 and a1surname!=None and authors[3]['first']!=None else None;
    a4first_  = authors[3]['first'] if len(authors)>3 else None;
    terms     = get_terms(title,cur_freq,con_freq);
    term1     = terms[0] if len(terms)>0 else None;
    term2     = terms[1] if len(terms)>1 else None;
    term3     = terms[2] if len(terms)>2 else None;
    term4     = terms[3] if len(terms)>3 else None;
    return (mentionID,pubID,dupID,1.0,title_,year,a1surname,a1init,a1first,a1first_,a2surname,a2init,a2first,a2first_,a3surname,a3init,a3first,a3first_,a4surname,a4init,a4first,a4first_,term1,term2,term3,term4,);

def generalize(row):
    mentionID,pubID,dupID,freq,title,year,a1surname,a1init,a1first,a1first_,a2surname,a2init,a2first,a2first_,a3surname,a3init,a3first,a3first_,a4surname,a4init,a4first,a4first_,term1,term2,term3,term4 = row;
    gens            = [];
    generalizations = [[a1surname,a1init,None,None,None,None,None,None,None,None,None,None,None,None,None,None],[None,None,None,None,a2surname,a2init,None,None,None,None,None,None,None,None,None,None],[None,None,None,None,None,None,None,None,a3surname,a3init,None,None,None,None,None,None],[None,None,None,None,None,None,None,None,None,None,None,None,a4surname,a4init,None,None]];
    for i in xrange(len(generalizations)):
        if generalizations[i][0+4*i] != None:
            gens.append(tuple([mentionID+'_'+str(i),pubID,dupID,0.0,None,None]+generalizations[i]+[None,None,None,None]));
    return gens;

def similarize(row):
    mentionID,pubID,dupID,freq,title,year,a1surname,a1init,a1first,a1first_,a2surname,a2init,a2first,a2first_,a3surname,a3init,a3first,a3first_,a4surname,a4init,a4first,a4first_,term1,term2,term3,term4 = row;
    optionss     = [els for els in itertools.product(*[[(term__,_transitions[term_][term__],) for term__ in _transitions[term_]] if term_ in _transitions else [(term_,1.,)] for term_ in [term1,term2,term3,term4]])];
    componentss  = [[part if not part==[] else None for part in option] for option in itertools.product(*[ [[term for term,weight in representation] for representation in component] for component in optionss])]
    weights      = [reduce(operator.mul,[weight for part in option for weight in part],1) for option in itertools.product(*[ [[weight for term,weight in representation] for representation in component] for component in optionss])]
    all_rows     = [tuple([mentionID,pubID,dupID,weights[i],title,year,a1surname,a1init,a1first,a1first_,a2surname,a2init,a2first,a2first_,a3surname,a3init,a3first,a3first_,a4surname,a4init,a4first,a4first_]+componentss[i]) for i in xrange(len(componentss))];
    return all_rows;

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
            print 'Cannot take integer of', first;
    return year;

def get_authors(authorlist):
    authors = [];
    for authorname in authorlist:
        parts      = authorname.lower().split(', ');
        surname    = parts[0];
        firstnames = parts[1].replace('.','').split(' ') if len(parts)>1 else [];
        firstname  = firstnames[0] if len(firstnames)>0 else None;
        firstinit  = firstname[0]  if firstname != None and firstname!='' else None;
        authors.append({'surname':surname, 'first':firstname, 'init':firstinit});
    return authors;

def get_terms(title,cur_freq,con_freq):
    if title == None:
        return [];
    terms = [term.lower() for term in re.findall(WORD,title)];#[_transform[term] if term in _transform else term for term in [term.lower() for term in re.split(r'(\W)+']];
    terms = sorted([(get_freq(term,cur_freq,con_freq),term,) for term in terms,title) if not term in _stopwords and len(term)>2]);
    return [term[1] for term in terms];

def get_freq(term,cur,con):
    global _freqs;
    if _simple_freqs:
        return 1.0;
    freq = 0.0;
    if not term in _freqs:
        row = cur.execute("SELECT freq FROM terms WHERE term=?",(term,)).fetchall();
        if len(row) > 0 and row[0][0] != None:
            freq = row[0][0];
        _freqs[term] = freq;
    else:
        freq = _freqs[term];
    return freq;

def get_freq_(term,cur,con):
    global _freqs;
    if _simple_freqs:
        return 1.0;
    freq = 0.0;
    if not term in _freqs:
        #return 0.0; # TODO: This was to avoid the requests call which is broken
        response = None;
        #------------------------------------------------------------------------------------------------
        while True:
            try:
                print 'Getting freq for', term,'from datamuse...';
                response = requests.get('https://api.datamuse.com/words?sp='+term+'&md=f&max=1').json();
            except requests.exceptions.HTTPError:
                print 'Could not get response. Sleep and retry...';
                time.sleep(_wait);
                continue;
            except:
                print 'Some non-http error occured. Skipping...';
                response = [];
                term     = term.encode('ascii','ignore');
            break;
        freq = 0.0 if len(response)==0 else float(response[0]['tags'][0][2:]);
        #------------------------------------------------------------------------------------------------ #TODO: This could also be moved to the end
        cur.execute("INSERT OR IGNORE INTO freqs(term,freq) VALUES(?,?)",(term,freq,));
        cur.execute("UPDATE freqs SET freq=? WHERE term=?",(freq,term,));
        #------------------------------------------------------------------------------------------------
        con.commit(); #TODO: Could be problematic!
        _freqs[term] = freq;
    else:
        freq = _freqs[term];
    return freq;

def work(Q,x):
    con      = sqlite3.connect(_outfolder+str(x)+'.db');
    cur      = con.cursor();
    con_freq = sqlite3.connect(_freqDB);
    cur_freq = con_freq.cursor();
    cur.execute('DROP TABLE IF EXISTS publications');
    cur.execute('CREATE TABLE publications(mentionID TEXT PRIMARY KEY, wos_id TEXT, id TEXT, freq REAL, title TEXT, year INT, a1sur TEXT, a1init TEXT, a1first TEXT, a1firstonly TEXT, a2sur TEXT, a2init TEXT, a2first TEXT, a2firstonly TEXT, a3sur TEXT, a3init TEXT, a3first TEXT, a3firstonly TEXT, a4sur TEXT, a4init TEXT, a4first TEXT, a4firstonly TEXT, term1 TEXT, term2 TEXT, term3 TEXT, term4 TEXT)');
    while not Q.empty():
        print 'Approximate number of jobs in queue:', Q.qsize();
        filename = Q.get(timeout=60);
        process(filename,cur,con,cur_freq,con_freq);
    con.close();

def feed(Q,infolder):
    for filename in os.listdir(infolder):
        if not filename.endswith('.json'):
            continue;
        Q.put(filename);
        while Q.qsize() > 10000:
            time.sleep(5);

#-----------------------------------------------------------------------------------------------------------------------
con_freq = sqlite3.connect(_freqDB);
cur_freq = con_freq.cursor();

cur_freq.execute("CREATE TABLE IF NOT EXISTS freqs(term TEXT PRIMARY KEY, freq REAL)");

freqs  = {term.encode('utf-8'): freq for term,freq in cur_freq.execute("SELECT term,freq FROM freqs") if freq!=None};
M      = MP.Manager();
_freqs = M.dict();

for key,value in freqs.iteritems():
    _freqs[key] = value;

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
