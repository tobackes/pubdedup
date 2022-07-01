import sqlite3
import sys
import asciidammit as dammit

_inDB  = sys.argv[1];
_outDB = sys.argv[2];

_grams   = True;
_n       = 5;
_wordsep = False;

con_in  = sqlite3.connect(_inDB);
cur_in  = con_in.cursor();
con_out = sqlite3.connect(_outDB);
cur_out = con_out.cursor();

def get_char_ngrams(title,n=4,wordsep=False):
    if title == None:
        return [];
    title  = dammit.asciiDammit(title.lower().replace(' ','_'));
    words  = [word.ljust(n,'_') for word in title.split('_')] if wordsep else [title];
    ngrams = [];
    for word in words:
        ngrams += [word[i:i+n] for i in range(len(word)-(n-1))];
    return ngrams;

def process(cur): #TODO: Test!
    if not _grams:
        for row in cur:
            yield row;
    else:
        for mentionIDIndex,paperIDIndex,rIDIndex,l,l_,f1,f1_,f2,f2_,f3,f3_ in cur:
            relevant = [];
            if l_==None and l!=None:
                relevant.append(l);
            elif l_!=None:
                relevant.append(l_);
            if f1_==None and f1!=None:
                relevant.append(f1);
            elif f1_!=None:
                relevant.append(f1_);
            if f2_==None and f2!=None:
                relevant.append(f2);
            elif f2_!=None:
                relevant.append(f2_);
            if f3_==None and f3!=None:
                relevant.append(f3);
            elif f3_!=None:
                relevant.append(f3_);
            name                      = '_'.join([part for part in relevant]);
            ngrams                    = get_char_ngrams(name,_n,_wordsep);
            l,l_,f1,f1_,f2,f2_,f3,f3_ = ngrams[:8] if len(ngrams) >= 8 else ngrams+[None for x in range(8-len(ngrams))];
            #print(mentionIDIndex,paperIDIndex,rIDIndex,l,l_,f1,f1_,f2,f2_,f3,f3_)
            yield mentionIDIndex,paperIDIndex,rIDIndex,l,l_,f1,f1_,f2,f2_,f3,f3_;

cur_in.execute("SELECT mentionIDIndex,paperIDIndex,rIDIndex,l,l_,f1,f1_,f2,f2_,f3,f3_ FROM names");

cur_out.execute("DROP TABLE IF EXISTS mentions");
cur_out.execute("CREATE TABLE mentions(mentionID INT, originalID TEXT, goldID TEXT, freq REAL, l TEXT, l_ TEXT, f1 TEXT, f1_ TEXT, f2 TEXT, f2_ TEXT, f3 TEXT, f3_ TEXT)");

cur_out.executemany("INSERT INTO mentions VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",((mentionIDIndex,paperIDIndex,rIDIndex,1.0,l,l_,f1,f1_,f2,f2_,f3,f3_,) for mentionIDIndex,paperIDIndex,rIDIndex,l,l_,f1,f1_,f2,f2_,f3,f3_ in process(cur_in)));

cur_out.execute("CREATE UNIQUE INDEX mentionID_index ON mentions(mentionID)");

con_out.commit();
con_out.close();
con_in.close();
