import sqlite3
import sys
import multiprocessing as MP
import time
import re
from collections import Counter
from elasticsearch import Elasticsearch as ES
import itertools
import operator
from functools import reduce

_outfolder = sys.argv[1];
_modfile   = sys.argv[2];
_trafile   = sys.argv[3];
_termFreqs = sys.argv[4];
_nameFreqs = sys.argv[5];
_jobs      = int(sys.argv[6]);

_simple_freqs = 1.0;
_scrollsize_  = 1000;

_dump      = 10000;
_wait      = 0.001;
_stopwords = ['the','a','of','with','to','where','by','as'];
_surpres   = set(['de','del','di','de la','von','van','della']);

WORD = re.compile(r'(\b[^\s]+\b)');
SUBS = re.compile(r'([A-Za-z0-9]|\s)+');
PUNC = re.compile(r'((\s-)|\.\s|\(|\)|\[|\]|:|;)');

_transitions = dict();
MOD          = open(_trafile,'r');
for line in MOD:
    term1,term2,weight = line.rstrip().split(' ');
    if term1 in _transitions:
        _transitions[term1][term2] = float(weight);
    else:
        _transitions[term1] = {term2:float(weight)};
MOD.close();

MOD          = open(_modfile,'r');
_transforms  = {line.rstrip().split(' ')[0]:line.rstrip().split(' ')[1] for line in MOD};
MOD.close();


def process(page,cur,con,x):
    rows = [];
    i    = 0;
    for result in page['hits']['hits']:
        i   += 1;
        row  = parse(result['_source']);
        sims = similarize(row);
        for sim in sims:
            gens  = generalize(sim);
            rows += [sim] + gens;
        if i % _dump == 0:
            print(i);
            cur.executemany("INSERT INTO publications VALUES("+','.join(['?' for el in rows[-1]])+")",rows);
            con.commit();
            rows = [];
    print(i);
    if rows != []:
        cur.executemany("INSERT INTO publications VALUES("+','.join(['?' for el in rows[-1]])+")",rows);
    con.commit();

def parse(entry): #TODO: Specify here how to generalize the available data to produce the required output features #TODO: Make sure the rows are in the same order as the table definition
    mentionID = entry['id'];
    pubID     = entry['id'];
    dupID     = entry['id'];
    title     = entry['title'] if 'title' in entry else None;
    title_    = ' '.join([term.lower() for term in title.split()]) if title != None else None;
    date      = entry['date'] if 'date' in entry else None;
    year      = get_year(date);
    persons   = entry['person'] if 'person' in entry else [];
    authors   = get_authors(persons);
    a1surname = authors[0]['surname']             if len(authors)>0 else None;
    a1init    = a1surname+'_'+authors[0]['init']  if len(authors)>0 and a1surname!=None and a1surname != None and authors[0]['init'] !=None else None;
    a1first   = a1surname+'_'+authors[0]['first'] if len(authors)>0 and a1surname!=None and a1surname != None and authors[0]['first']!=None else None;
    a1first_  = authors[0]['first'] if len(authors)>0 else None;
    a2surname = authors[1]['surname']             if len(authors)>1 else None;
    a2init    = a2surname+'_'+authors[1]['init']  if len(authors)>1 and a1surname!=None and a2surname != None and authors[1]['init'] !=None else None;
    a2first   = a2surname+'_'+authors[1]['first'] if len(authors)>1 and a1surname!=None and a2surname != None and authors[1]['first']!=None else None;
    a2first_  = authors[1]['first'] if len(authors)>1 else None;
    a3surname = authors[2]['surname']             if len(authors)>2 else None;
    a3init    = a3surname+'_'+authors[2]['init']  if len(authors)>2 and a1surname!=None and a3surname != None and authors[2]['init'] !=None else None;
    a3first   = a3surname+'_'+authors[2]['first'] if len(authors)>2 and a1surname!=None and a3surname != None and authors[2]['first']!=None else None;
    a3first_  = authors[2]['first'] if len(authors)>2 else None;
    a4surname = authors[3]['surname']             if len(authors)>3 else None;
    a4init    = a4surname+'_'+authors[3]['init']  if len(authors)>3 and a1surname!=None and a4surname != None and authors[3]['init'] !=None else None;
    a4first   = a4surname+'_'+authors[3]['first'] if len(authors)>3 and a1surname!=None and a4surname != None and authors[3]['first']!=None else None;
    a4first_  = authors[3]['first'] if len(authors)>3 else None;
    terms     = get_subphrases(title);
    term1     = terms[0] if len(terms)>0 else None;
    term2     = terms[1] if len(terms)>1 else None;
    term3     = terms[2] if len(terms)>2 else None;
    term4     = terms[3] if len(terms)>3 else None;
    return (mentionID,pubID,dupID,1.0,title_,year,a1surname,a1init,a1first,a1first_,a2surname,a2init,a2first,a2first_,a3surname,a3init,a3first,a3first_,a4surname,a4init,a4first,a4first_,term1,term2,term3,term4,);

def generalize(row):
    mentionID,pubID,dupID,freq,title,year,a1surname,a1init,a1first,a1first_,a2surname,a2init,a2first,a2first_,a3surname,a3init,a3first,a3first_,a4surname,a4init,a4first,a4first_,term1,term2,term3,term4 = row;
    generalizations = set([]);
    for author_i in range(5): #TODO: would be better to simply load the authors and terms and then create the subsets and recreate row
        for term_i in range(5):
            author_pos        = set([6  + author_j * 4 for author_j in range(4) if author_j != author_i]); #all authors but author_i
            term_pos          = set([22 + term_j       for term_j   in range(4) if term_j != term_i]); #all terms but term_i
            date_pos          = set([]) if term_i == 4 or author_i == 4 else set([5]); #only drop year if no author is dropped or no term is dropped
            used_pos          = author_pos | term_pos | date_pos;
            generalization    = tuple([row[pos] if pos in used_pos else None for pos in range(len(row))]);
            if (generalization[6] != None or generalization[10] != None or generalization[14] != None or generalization[18] != None) and (generalization[22] != None or generalization[23] != None or generalization[24] != None or generalization[25] != None):
                generalizations.add(generalization);
    generalizations_ = []; i = 0;
    for gen in generalizations:
        generalization    = list(gen);
        i                += 1;
        generalization[0] = row[0]+'_'+str(i);
        generalization[1] = row[1];
        generalization[3] = 0.0;
        generalizations_.append(generalization);
    return generalizations_;

def similarize(row):
    mentionID,pubID,dupID,freq,title,year,a1surname,a1init,a1first,a1first_,a2surname,a2init,a2first,a2first_,a3surname,a3init,a3first,a3first_,a4surname,a4init,a4first,a4first_,term1,term2,term3,term4 = row;
    represents   = [[(term,'title')] for term in [term1,term2,term3,term4] if term != None];
    represents   = [[term1,term2,term3,term4]]; #TODO: Could add more features like [,term4],[a1surname,a2...]
    optionss     = [[els for els in itertools.product(*[[(term2,_transitions[term1][term2],) for term2 in _transitions[term1]] if term1 in _transitions else [(term1,1.,)] for term1 in represents[i]])] for i in range(len(represents))];
    componentss  = [[part if not part==[] else None for part in option] for option in itertools.product(*[ [[term for term,weight in representation] for representation in component] for component in optionss])];
    weights      = [reduce(operator.mul,[weight for part in option for weight in part],1) for option in itertools.product(*[ [[weight for term,weight in representation] for representation in component] for component in optionss])]
    all_rows     = [tuple([mentionID+'_'+str(i),pubID,dupID,weights[i],title,year,a1surname,a1init,a1first,a1first_,a2surname,a2init,a2first,a2first_,a3surname,a3init,a3first,a3first_,a4surname,a4init,a4first,a4first_]+componentss[i][j]) for i in range(len(componentss)) for j in range(len(componentss[i]))]; #TODO: What if j is not always 0???
    return all_rows;

def get_year(date): #TODO: Adapt this to the GESIS data
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

def get_authors(authorlist): #TODO: Adapt to gesis data
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

def get_bigrams(title):
    words = title.split();
    return [words[i]+' '+words[i+1] for i in range(len(words)-1)] if len(words) > 0 else [];

def get_subphrases(title):
    if not isinstance(title,str):
        return [];
    #return [match.group(0).lower().strip() for match in SUBS.finditer(title)];
    parts = [phrase.lower().strip().replace('"','').replace("'",'') for phrase in re.split(PUNC,title) if phrase != None];
    return [part for part in parts if not re.match(PUNC,part) and len(part) > 1];

def get_terms(title):
    if title == None:
        return [];
    terms = [term.lower() for term in re.findall(WORD,title)];#[_transform[term] if term in _transform else term for term in [term.lower() for term in re.split(r'(\W)+']];
    terms = sorted([(_freq_term[term],term,) for term in terms if not term in _stopwords and len(term)>2]);
    return [_transforms[term] if term in _transforms else term for freq,term in terms];

def get(Q,timeout=1):
    try_time = 0;
    start    = time.process_time();
    while True:
        try:
            return Q.get(block=False);
        except:
            if try_time > timeout:
                return None;
            else:
                try_time += time.process_time() - start;
                time.sleep(0.01);
                pass;

def work(Q,x):
    con        = sqlite3.connect(_outfolder+str(x)+'.db');
    cur        = con.cursor();
    cur.execute('DROP TABLE IF EXISTS publications');
    cur.execute('CREATE TABLE publications(mentionID TEXT PRIMARY KEY, wos_id TEXT, id TEXT, freq REAL, title TEXT, year INT, a1sur TEXT, a1init TEXT, a1first TEXT, a1firstonly TEXT, a2sur TEXT, a2init TEXT, a2first TEXT, a2firstonly TEXT, a3sur TEXT, a3init TEXT, a3first TEXT, a3firstonly TEXT, a4sur TEXT, a4init TEXT, a4first TEXT, a4firstonly TEXT, term1 TEXT, term2 TEXT, term3 TEXT, term4 TEXT)');
    while True:
        print('Approximate number of jobs in queue:', Q.qsize());
        page = get(Q);
        if page == None:
            break;
        process(page,cur,con,x);
    con.close();

def feed(Q):
    gate       = 'search.gesis.org/es-config/';
    addr_index = 'gesis-test';
    addr_body  = { "query": { "bool": { "must": [{ "term": { "_type": "publication" } }] } } , "_source": ["title","date","person","coreEditor","coreSatit","data_source","id"] };
    client     = ES([gate],scheme='http',port=80,timeout=60);
    page       = client.search(index=addr_index,body=addr_body,scroll='2m',size=_scrollsize_);
    sid        = page['_scroll_id'];
    size       = float(page['hits']['total']);
    returned   = size;
    page_num   = 0;
    while (returned > 0):
        page_num  += 1;
        page       = client.scroll(scroll_id=sid, scroll='2m');
        returned   = len(page['hits']['hits']);
        if returned == 0: break;
        #while Q.qsize()>1000000/_scrollsize_:
        #    time.sleep(1);
        Q.put(page);

#-----------------------------------------------------------------------------------------------------------------------
con_freq   = sqlite3.connect(_termFreqs);
cur_freq   = con_freq.cursor();
_freq_term = Counter({term: freq for term,freq in cur_freq.execute("SELECT term,freq FROM terms") if freq!=None});
con_freq.close();
#-----------------------------------------------------------------------------------------------------------------------
con_freq  = sqlite3.connect(_nameFreqs);
cur_freq  = con_freq.cursor();
_freq_sur = Counter({name: freq for name,freq in cur_freq.execute("SELECT name,freq FROM surnames   WHERE name IS NOT NULL")});
_freq_1st = Counter({name: freq for name,freq in cur_freq.execute("SELECT name,freq FROM firstnames WHERE name IS NOT NULL")});
con_freq.close();
#-----------------------------------------------------------------------------------------------------------------------

manager = MP.Manager();
Q       = manager.Queue();

feeder  = MP.Process(target=feed,args=(Q,));
workers = [MP.Process(target=work,args=(Q,x,)) for x in range(_jobs)];

feeder.start(); time.sleep(1);

for worker in workers:
    worker.start();

feeder.join();

for worker in workers:
    worker.join();
#-----------------------------------------------------------------------------------------------------------------------
