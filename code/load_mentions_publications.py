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
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.corpus import wordnet as WN
from nltk.stem.wordnet import WordNetLemmatizer
from unidecode import unidecode as UD
import asciidammit as dammit
import re
from symspellpy import SymSpell, Verbosity
import pkg_resources

_infolder  = sys.argv[1];
_outfolder = sys.argv[2];
_modfile   = sys.argv[3];
_termFreqs = sys.argv[4];
_nameFreqs = sys.argv[5];
_jobs      = int(sys.argv[6]);

_simple_freqs = 1.0;

_dump      = 10000;
_wait      = 0.001;

_dict_edit_dist = 4;
_ratio          = 0.2;

# The three cases used are <words,parts> / <word_ngrams,parts> / <char_ngrams,char_ngrams>
_termfeats       = 'char_grams_by_word' #'word_ngrams'; #'words' 'char_grams' 'char_grams_by_word'
_authfeats       = 'parts' #'parts' 'char_grams'
_wordsep_authors = True;
_n_authors       = 5;
_n_terms         = 5;

_sym_spell       = SymSpell(max_dictionary_edit_distance=_dict_edit_dist, prefix_length=7);
_dictionary_path = pkg_resources.resource_filename("symspellpy", "frequency_dictionary_en_82_765.txt");

_sym_spell.load_dictionary(_dictionary_path, term_index=0, count_index=1);

_stopwords = set().union(*[set(stopwords.words(lang)) for lang in ['english','german','french','italian','spanish','russian','portuguese','dutch','swedish','danish','finnish']]);
_tokenizer = RegexpTokenizer(r'\w+')
_surpres   = set(['de','del','di','de la','von','van','della']);

NONAME    = re.compile(r'(.*anonym\w*)|(.*unknown\w*)|(\s*-\s*)');
WORD      = re.compile(r'(\b[^\s]+\b)'); #TODO: Make stricter
STRIP     = re.compile(r'(^(\s|,)+)|((\s|,)+$)');
PUNCT     = re.compile(r'[!"#$%&\'()*+\/:;<=>?@[\\\]^_`{|}~1-9]'); #Meaningless punctuation for Author name lists, excludes , . -
SUBTITDIV = re.compile(r'\. |: | -+ |\? ');
STOPWORDS = re.compile(r'&|\.|\,|'+r'|'.join(['\\b'+stopword+'\\b' for stopword in _stopwords]));

MOD          = open(_modfile,'r');
_transforms  = {line.rstrip().split(' ')[0]:line.rstrip().split(' ')[1] for line in MOD};
MOD.close();

WNL = WordNetLemmatizer();


def generalize(term):               #TODO: Try to improve this by somehow getting the synset counts from the online wordnet rather than the NLTK lemma counts
    for synset in WN.synsets(term): #TODO: Might also use the phrasefinder API to select one of multiple word unigrams or bigrams by the most frequent one
        yield synset;

def concat(object1, object2):
    if isinstance(object1, str):
        object1 = [object1]
    if isinstance(object2, str):
        object2 = [object2]
    return object1 + object2

def capitalize(word):
    return word[0].upper() + word[1:]

def is_word(string):
    return len(string) > 2 and (string in _stopwords or len(WN.synsets(string)) > 0 or len(_sym_spell.lookup(string, Verbosity.CLOSEST,max_edit_distance=0, include_unknown=False))>0);

def splitter(string, language='en_us'):
    for index, char in enumerate(string):
        left_compound         = string[0:-index];
        right_compound_1      = string[-index:];
        right_compound_2      = string[-index+1:];
        right_compound1_upper = right_compound_1[0].isupper() if right_compound_1 else None;
        right_compound2_upper = right_compound_2[0].isupper() if right_compound_2 else None;
        left_compound         = capitalize(left_compound) if index > 0 and len(left_compound) > 1 and not is_word(left_compound) else left_compound;
        left_compound_valid   = is_word(left_compound);
        #print(left_compound,right_compound_1,right_compound_2,right_compound1_upper,right_compound2_upper,left_compound,left_compound_valid);
        if left_compound_valid and ((not splitter(right_compound_1,language) == '' and not right_compound1_upper) or right_compound_1 == ''):
            return [compound for compound in concat(left_compound, splitter(right_compound_1, language)) if not compound == ''];
        if left_compound_valid and string[-index:-index+1] == 's' and ((not splitter(right_compound_2, language) == '' and not right_compound2_upper) or right_compound_2 == ''):
            return [compound for compound in concat(left_compound, splitter(right_compound_2, language)) if not compound == ''];
    return [string] if not string == '' and is_word(string) else [capitalize(string)] if not string == '' and is_word(capitalize(string)) else '';

def split(string, language='en_us'): # Only called for strings where not is_word
    if string in _stopwords or len(string) <= 2:
        return [string.lower()];
    parts = splitter(string,language);
    return [string.lower()] if len(parts)==0 else [part.lower() for part in parts];

def correct(string):
    suggestions = [suggestion.term for suggestion in _sym_spell.lookup(string, Verbosity.CLOSEST,max_edit_distance=min(_dict_edit_dist,int(len(string)*_ratio)), include_unknown=False)];
    string_     = suggestions[0] if len(suggestions) > 0 else string;
    #if string_ != string:
    #    print('Corrected',string,'to',string_);
    return string_;

def get_char_ngrams(title,n=4,wordsep=False):
    if title == None:
        return [];
    title  = dammit.asciiDammit(title.lower().replace(' ','_'));
    words  = title.split('_') if wordsep else [title];
    ngrams = [];
    for word in words:
        ngrams += [word[i:i+n] for i in range(len(word)-(n-1))];
    return ngrams;

def get_words(title):
    if title == None:
        return [];
    title = title.lower();
    words = [dammit.asciiDammit(word) for word in _tokenizer.tokenize(title) if not word in _stopwords];
    words = [correct(word) if not is_word(word) else word for word in words]; #TODO: Should stopwords and words shorter than 3 not be excluded?
    return words;

def get_word_ngrams(title):
    if title == None:
        return [];
    known_bi = [];
    unknown  = [];
    title    = title.lower();
    titles   = SUBTITDIV.split(title);
    sections = [division for title in titles for division in STOPWORDS.split(title)];
    for section in sections:
        words = [dammit.asciiDammit(word) for word in _tokenizer.tokenize(section) if not word in _stopwords];
        words = [correct(word) if not is_word(word) else word for word in words];
        known = [];
        for word in words:
            if is_word(word):
                known.append(word);
            else:
                subwords = split(word);
                if len(subwords) > 1:
                    known += subwords;
                else:
                    unknown += subwords;
        for i in range(len(known)):
            #synsets  = list(generalize(known[i]));
            #lemmas   = sorted([(lemma.count(),lemma.name(),) for synset in synsets for lemma in synset.lemmas()], reverse=True) if len(synsets) > 0 else [(0,known[i],)];
            posses   = [(sum([lemma.count() for lemma in synset.lemmas()]),synset.pos()) for synset in WN.synsets(known[i])]
            pos      = posses[-1][1] if len(posses) > 0 else None
            known[i] = WNL.lemmatize(known[i],pos) if pos != None else WNL.lemmatize(known[i]);#lemmas[0][1];
        bigrams   = [known[i]+' '+known[i+1] for i in range(len(known)-1)] if len(known) > 1 else known;
        known_bi += bigrams;
    terms  = known_bi + unknown;
    check  = set([]);
    terms_ = [];
    for term in terms:
        if not term in check:
            terms_.append(term);
            check.add(term);
    return terms_;

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

def authgrams(a1surname,a1init,a1first,a2surname,a2init,a2first,a3surname,a3init,a3first,a4surname,a4init,a4first):
    a1s                       = '_'.join((el for el in [a1surname,a1init,a1first] if el));
    a2s                       = '_'.join((el for el in [a2surname,a2init,a2first] if el));
    a3s                       = '_'.join((el for el in [a3surname,a3init,a3first] if el));
    a4s                       = '_'.join((el for el in [a4surname,a4init,a4first] if el));
    a1grams                   = get_char_ngrams(a1s,_n_authors,_wordsep_authors);
    a2grams                   = get_char_ngrams(a2s,_n_authors,_wordsep_authors);
    a3grams                   = get_char_ngrams(a3s,_n_authors,_wordsep_authors);
    a4grams                   = get_char_ngrams(a4s,_n_authors,_wordsep_authors);
    agrams                    = a1grams + a2grams + a3grams + a4grams;
    return agrams[:12] if len(agrams) >= 12 else agrams+[None for x in range(12-len(agrams))]; # This means that longer names will overwrite later ones

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
    terms           = get_word_ngrams(title) if _termfeats=='word_ngrams' else get_words(title) if _termfeats=='words' else get_char_ngrams(title,_n_terms,False) if _termfeats=='char_grams' else get_char_ngrams(title,_n_terms,True) if _termfeats=='char_grams_by_word' else [];
    authfeats       = authgrams(a1surname,a1init,a1first,a2surname,a2init,a2first,a3surname,a3init,a3first,a4surname,a4init,a4first) if _authfeats=='char_grams' else [init for init in [a1init,a2init,a3init,a4init] if init!=None];
    author_ngrams   = authfeats[:min(len(authfeats),max(9,18-len(terms)))];
    term_ngrams     = terms[:min(len(terms),max(9,18-len(authfeats)))];
    ngramfeats      = author_ngrams + term_ngrams;
    ngramfeats_     = ngramfeats + [None for x in range(18-len(ngramfeats))];
    term1, term1gen = (terms[0],None) if len(terms)>0 else (None,None);
    term2, term2gen = (terms[1],None) if len(terms)>1 else (None,None);
    term3, term3gen = (terms[2],None) if len(terms)>2 else (None,None);
    term4, term4gen = (terms[3],None) if len(terms)>3 else (None,None);
    term5, term5gen = (terms[4],None) if len(terms)>4 else (None,None);
    term6, term6gen = (terms[5],None) if len(terms)>5 else (None,None);
    firstvals       = [mentionID,pubID,dupID,1.0,title_,year1,year2]; # Below we have 18 features available
    nextvals        = [a1surname,a1init,a1first,a2surname,a2init,a2first,a3surname,a3init,a3first,a4surname,a4init,a4first,term1,term2,term3,term4,term5,term6] if _authfeats=='parts' and (_termfeats=='words' or _termfeats=='word_ngrams') else authfeats[:12] + [term1,term2,term3,term4,term5,term6] if _termfeats=='words' or _termfeats=='word_ngrams' else ngramfeats;
    restvals        = [terms[i] if i < len(terms) else None for i in range(len(term_ngrams),len(term_ngrams)+(24-len(ngramfeats)))] if _termfeats=='char_grams' or _termfeats=='char_grams_by_word' else [term1gen,term2gen,term3gen,term4gen,term5gen,term6gen];
    #print(ngramfeats); print(author_ngrams); print(term_ngrams); print(restvals); print('--------------------------------------');
    values          = firstvals + nextvals + restvals;
    return tuple([val if val!='' else None for val in values]);

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

def get_authors(authorlist): #TODO: ASCIIdammit missing! #TODO: There are still surnames and firstnames with only one letter! #TODO: THe code should be made more efficient
    authors    = [];
    authorlist = [PUNCT.sub('',dammit.asciiDammit(entry)) for entry in authorlist];
    for authorname in authorlist:
        authorname = re.sub(STRIP,'',authorname); #remove beginning or ending whitespace or comma
        parts      = authorname.split(',') if ',' in authorname else [authorname.split(' ')[-1],' '.join(authorname.split(' ')[:-1])];
        parts      = [part.strip() for part in parts];
        surname    = parts[0].lower();
        firstnames = [el for el in parts[1].replace('.',' ').split(' ') if not el==''] if len(parts)>1 else [];
        firstname  = firstnames[0].lower() if len(firstnames)>0 and len(firstnames[0]) > 2 else None;
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
            firstname  = firstnames[0].lower() if len(firstnames)>0 and len(firstnames[0]) > 2 else None;
            firstinit  = firstname[0] if firstname != None else None;
            surname    = ' '.join(surnames);
        if surname != None and firstname != None and len(surname) == 1:# and _freq_1st[firstname] < _freq_sur[firstname]:
            temp      = surname;
            surname   = firstname;
            firstname = temp;
            firstinit = firstname[0];
            #print('surname only one letter and firstname more frequent as surname:');
            #print('surname:', surname, '--- firstname:', firstname, '--- firstinit:', firstinit);
        if surname != None and firstname != None and len(firstname)>2 and len(surname)>2 and _freq_sur[surname] < _freq_1st[surname] and _freq_1st[firstname] < _freq_sur[firstname]:
            temp      = surname;
            surname   = firstname;
            firstname = temp;
            firstinit = firstname[0];
            #print('surname more frequent as firstname and firstname more frequent as surname:');
            #print('surname:', surname, '--- firstname:', firstname, '--- firstinit:', firstinit);
            #print(surname, _freq_sur[surname], firstname, _freq_1st[firstname]);
        if surname != None and '.' in surname:
            firstinit = surname.replace('.','')[0] if len(surname.replace('.',''))>0 else None;
            temp      = firstname;
            firstname = surname.replace('.','') if len(surname.replace('.','')) > 2 else None;
            surname   = temp;
        surname   = ascii(shorten(surname));
        firstinit = ascii(firstinit);
        firstname = ascii(firstname);
        surname   = None if (not isinstance(surname,str)  ) or NONAME.match(surname.lower())   else surname;
        firstinit = None if (not isinstance(firstinit,str)) or NONAME.match(firstinit.lower()) else firstinit;
        firstname = None if (not isinstance(firstname,str)) or NONAME.match(firstname.lower()) else firstname;
        authors.append({'string':authorname,'surname':surname, 'first':firstname, 'init':firstinit});
    return authors;

def shorten(surname): # Not optimal for Spanish names, also not for something like Smith C
    surname_ = surname.strip().split()[-1] if surname!=None and surname!='' else None;
    return surname if surname_==None or len(surname_)==0 else surname_;

def ascii(term):
    term_ = UD(term).replace("'",'') if term!=None else None;
    return term if term_==None or len(term_)==0 else term_;

def get_terms_(title):
    if title == None:
        return [];
    terms = [term.lower() for term in re.findall(WORD,title)];#[_transform[term] if term in _transform else term for term in [term.lower() for term in re.split(r'(\W)+']];
    terms = sorted([(_freq_term[term],term,) for term in terms if not term in _stopwords and len(term)>2]);
    return [(term,_transforms[term],) if term in _transforms else (term,term,) for freq,term in terms];

def work(Q,x):
    con      = sqlite3.connect(_outfolder+str(x)+'.db');
    cur      = con.cursor();
    cur.execute('DROP TABLE IF EXISTS mentions');
    cur.execute('CREATE TABLE mentions(mentionID INT, originalID TEXT, goldID TEXT, freq REAL, title TEXT, year1 INT, year2 INT, a1sur TEXT, a1init TEXT, a1first TEXT, a2sur TEXT, a2init TEXT, a2first TEXT, a3sur TEXT, a3init TEXT, a3first TEXT, a4sur TEXT, a4init TEXT, a4first TEXT, term1 TEXT, term2 TEXT, term3 TEXT, term4 TEXT, term5 TEXT, term6 TEXT, term1gen TEXT, term2gen TEXT, term3gen TEXT, term4gen TEXT, term5gen TEXT, term6gen TEXT)');
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
