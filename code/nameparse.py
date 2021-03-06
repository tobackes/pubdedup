import sys
import sqlite3
from collections import Counter

_name   = sys.argv[1];
_freqDB = sys.argv[2];

def get_authors(authorlist):
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
        if surname != None and firstname != None and _freq_sur[surname] < _freq_1st[surname] and _freq_1st[firstname] < _freq_sur[firstname]:
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

con_freq = sqlite3.connect(_freqDB);
cur_freq = con_freq.cursor();

_freq_sur  = Counter({name.encode('utf-8'): freq for name,freq in cur_freq.execute("SELECT name,freq FROM surnames   WHERE name IS NOT NULL")});
_freq_1st  = Counter({name.encode('utf-8'): freq for name,freq in cur_freq.execute("SELECT name,freq FROM firstnames WHERE name IS NOT NULL")});

con_freq.close();

authors = get_authors([_name]);

print authors;
