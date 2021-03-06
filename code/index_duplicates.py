import sys, os
import time
import sqlite3
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import parallel_bulk as bulk

_infiles = sys.argv[1::2];
_intypes = sys.argv[2::2];

_header = ['id','duplicates','min_els','class','data_type'];

_index = 'gws-duplicates-'+time.ctime(time.time()).replace(' ','-').replace(':','').lower();

_body = {   '_op_type':  'index',
            '_index':    _index,
            '_id':       None,
            '_source':   { field:None for field in _header },
            '_type':     'duplicate'
        }

def make_json(repID):
    string = repID.replace('\r\n',' ').replace('\n',' ').replace('\r',' ').replace('\\','');
    obj    = {};
    try:
        obj = json.loads(string);
    except:
        print('---ERROR--->',string);
        pass;
    return obj;

def get_duplicates(infiles,types):
    for x in range(len(infiles)):
        con    = sqlite3.connect(infiles[x]);
        cur    = con.cursor();
        labels = [row[0] for row in cur.execute('SELECT DISTINCT label FROM labelling').fetchall()];
        for label in labels:
            ids                           = [row[0]            for row in cur.execute('SELECT id    FROM labelling WHERE label=?',(label,)).fetchall()];
            repIDs                        = [make_json(row[0]) for row in cur.execute('SELECT repID FROM min_els   WHERE label=?',(label,)).fetchall()];
            body                          = copy(_body);
            body['_id']                   = types[x]+'_'+str(label);
            body['_source']['data_type']  = types[x];
            body['_source']['id']         = types[x]+'_'+str(label);
            body['_source']['class']      = 'unclassified';
            body['_source']['duplicates'] = ids;
            body['_source']['min_els']    = repIDs;
            yield body;
        con.close();

client = ES(['search.gesis.org/es-config/'],scheme='http',port=80,timeout=60);

i = 0;
for success, info in bulk(client,get_duplicates(_infiles,_intypes)):
    i += 1;
    if not success:
        print('A document failed:', info['index']['_id'], info['index']['error']);
    elif i % 10000 == 0:
        print(i);

print('created new index', _index);

indices = set(client.indices.get('gws-duplicates-*')) & set(client.indices.get_alias("gws-duplicates*"));
for index in indices:
    if index != _index:
        print('...deleting old index', index);
        client.indices.delete(index=index, ignore=[400, 404]);

client.indices.put_alias(index=_index, name='gws-duplicates');
print('added alias "gws-duplicates" to index', _index);


