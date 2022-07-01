import sqlite3
import sys
import asciidammit as dammit

_inDB  = sys.argv[1];
_outDB = sys.argv[2];

_mode = 'ngrams' #'parts' #'ngrams' # fields and parts are the same, ngrams require to split the string into ngrams and insert them into the columns

con_in  = sqlite3.connect(_inDB);
cur_in  = con_in.cursor();
con_out = sqlite3.connect(_outDB);
cur_out = con_out.cursor();

columns = [row[1] for row in cur_in.execute("PRAGMA table_info(representations)").fetchall() if not row[1] in set(['rowid','mentionID','id','string'])];
colstr  = ','.join(columns);


def get_char_ngrams(title,n=4,wordsep=False):
    if title == None:
        return [];
    title  = dammit.asciiDammit(title.lower().replace(' ','_'));
    words  = title.split('_') if wordsep else [title];
    ngrams = [];
    for word in words:
        ngrams += [word[i:i+n] for i in range(len(word)-(n-1))];
    return ngrams;

def process(cur): #TODO: This does not work at all
    if _mode == 'ngrams': # rowid,mentionID,id,1,string
        for row in cur:
            ngrams = get_char_ngrams(row[4]);
            row_   = list(row)[:6]+ngrams[:len(row)-6];
            yield tuple( row_ + [None for i in range(len(row)-len(row_))] );
    else:
        for row in cur:
            yield row;


cur_in.execute("SELECT rowid,mentionID,id,1,string,"+colstr+" FROM representations");

cur_out.execute("DROP TABLE IF EXISTS mentions");
cur_out.execute("CREATE TABLE mentions(mentionID INT, originalID TEXT, goldID TEXT, freq REAL, string TEXT,"+colstr+")");

cur_out.executemany("INSERT INTO mentions VALUES("+','.join(['?' for i in range(5+len(columns))])+")",process(cur_in));

cur_out.execute("CREATE UNIQUE INDEX mentionID_index ON mentions(mentionID)");

con_out.commit();
con_out.close();
con_in.close();
