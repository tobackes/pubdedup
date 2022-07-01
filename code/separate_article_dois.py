import sys
import sqlite3
#import crossref_commons.retrieval as CR
from crossref.restful import Works, Etiquette
import time

_doi_db = sys.argv[1];


userinfo  = Etiquette('ProjectX', '1.0', None, 'nasibaer@live.de')
interface = Works(etiquette=userinfo);

con = sqlite3.connect(_doi_db);
cur = con.cursor();

types = [];

cur.execute("SELECT doi FROM dois WHERE legal");

i = 0;
for row in cur:
    i       += 1;
    if i % 10 == 0:
        print(i,end='\r');
    doi      = row[0];
    metadata = {'type':None};
    t        = time.time();
    try:
        metadata = interface.doi(doi);#CR.get_publication_as_json(doi);
    except KeyboardInterrupt:
        break;
    except Exception as e:
        print(e);
        print('Problem accessing API for doi',doi);
        pass;
    response_time = time.time()-t; print(response_time);
    typ           = metadata['type'] if metadata and 'type' in metadata else None;
    types.append([doi,typ]);


