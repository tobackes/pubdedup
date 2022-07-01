import os
import sqlite3
import json
from copy import deepcopy as copy
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import parallel_bulk as bulk

_max_size    = 5;
_min_sim     = 0.9;
_max_time    = 1;
_diff_source = True;


_index     = "gws-duplicates";
_index_pub = "gesis-test";

_body_duplicates = {
                     "query":   { "match_all": {} },
                     "_source": ["id","duplicates"]
                   };

_body_pub        = {
                     "query":   { 'ids': { 'values': None } },
                     "_source": ["title","date","index_source"]
                   };

_body_update = { '_index':   _index,
                 '_type':    'duplicate',
                 '_op_type': 'update',
                 '_id':      None,
                 'doc':      { 'class': None }
               };

_client = ES(['search.gesis.org/es-config/'],scheme='http',port=80,timeout=60);
_batch  = 100;


def true_duplicates(client,index,body,batch):
    #print('Start scrolling...');
    results   = client.search(index=index,scroll='2m',size=batch,body=body);
    sid       = results['_scroll_id'];
    length    = len(results['hits']['hits']);
    processed = 0;
    while length > 0:
        processed += length;
        #print('Number of items scrolled over:', processed);
        #--------------------------------------------------------------
        for result in results['hits']['hits']:
            if len(result['_source']['duplicates']) <= _max_size:
                groupID                            = result['_source']['id'];
                body_pub                           = copy(_body_pub);
                body_pub['query']['ids']['values'] = result['_source']['duplicates'];
                results_pub                        = client.search(index=_index_pub,body=body_pub);
                duplicates                         = [(hit['_source']['title'] if 'title' in hit['_source'] else None,get_year(hit['_source']['date']) if 'year' in hit['_source'] else None,hit['_source']['index_source'] if 'index_source' in hit['_source'] else None,) for hit in results_pub['hits']['hits']];
                if verified(duplicates):
                    body_update = copy(_body_update);
                    body_update['_id'] = groupID;
                    body_update['doc']['class'] = 'verified';
                    yield body_update;
        #--------------------------------------------------------------
        results = client.scroll(scroll_id=sid, scroll='2m');
        sid     = results['_scroll_id'];
        length  = len(results['hits']['hits']);

def verified(duplicates):
    min_similar = 1; TITLE  = 0;
    max_yeardif = 0; YEAR   = 1;
    min_srcdif  = 1; SOURCE = 2;
    for i in range(len(duplicates)-1):
        for j in range(i+1,len(duplicates)): #TODO: Could break if condition cannot be met anymore
            tit_i,tit_j = duplicates[i][TITLE][0] if isinstance(duplicates[i][TITLE],list) else duplicates[i][TITLE].lower(), duplicates[j][TITLE][0] if isinstance(duplicates[j][TITLE],list) else duplicates[j][TITLE].lower();
            damerau_sim = 1 - damerau_dist(tit_i,tit_j);
            prefix_len  = len(os.path.commonprefix([tit_i,tit_j]));
            combine_sim = max(damerau_sim, (1.*prefix_len)/min(len(tit_i),len(tit_j)));
            #print(tit_i); print(tit_j); print(combine_sim); print('-------------------------------------------');
            year_diff   = 0 if duplicates[i][YEAR]  ==None or duplicates[j][YEAR]  ==None else abs(duplicates[i][YEAR]-duplicates[j][YEAR]);
            source_diff = 1 if duplicates[i][SOURCE]==None or duplicates[j][SOURCE]==None else duplicates[i][SOURCE] != duplicates[j][SOURCE];
            if combine_sim < min_similar:
                min_similar = combine_sim;
            if year_diff > max_yeardif:
                max_yeardif = year_diff;
            if source_diff < min_srcdif:
                min_srcdif = source_diff;
    #        print(i,j, ':', duplicates[i][SOURCE], duplicates[j][SOURCE]);
    #print('-----------------------------------------------------------------------');
    #print(min_similar >= _min_sim , max_yeardif <= _max_time , min_srcdif >= _diff_source);
    #print('-----------------------------------------------------------------------');
    return min_similar >= _min_sim and max_yeardif <= _max_time and min_srcdif >= _diff_source;

def damerau_dist(s1,s2):
    oneago  = None;
    thisrow = list(range(1,len(s2)+1))+[0];
    for x in range(len(s1)):
        twoago, oneago, thisrow = oneago, thisrow, [0]*len(s2)+[x + 1];
        for y in range(len(s2)):
            delcost    = oneago[y] + 1;
            addcost    = thisrow[y-1] + 1;
            subcost    = oneago[y-1] + (s1[x]!=s2[y]);
            thisrow[y] = min(delcost,addcost,subcost);
            if (x>0 and y>0 and s1[x]==s2[y-1] and s1[x-1]==s2[y] and s1[x]!=s2[y]):
                thisrow[y] = min(thisrow[y],twoago[y-2]+1);
    sim = thisrow[len(s2)-1]/(1.*max(len(s1),len(s2)));
    return sim;

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


i = 0;
for success, info in bulk(_client,true_duplicates(_client,_index,_body_duplicates,_batch)):
    i += 1;
    if not success:
        print('A document failed:', info['index']['_id'], info['index']['error']);
    elif i % 100 == 0:
        print(i, 'updated');

print('updated index', _index);
