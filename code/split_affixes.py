from collections import Counter
from collections import defaultdict
import os,sys
import re, regex
import csv
import itertools
import functools
import operator
from copy import deepcopy as copy
import sqlite3
import numpy as np
from scipy.sparse import csr_matrix as csr
from scipy.sparse.csgraph import connected_components
import multiprocessing as MP
import time
import fasttext

_termfile = sys.argv[1];
_outfile  = sys.argv[2];

_model = 'resources/lid.176.bin';

_threshold_prefix  = 0.75#0.8; #TODO: These thresholds might be different for each typ
_window_prefix     = 1;
_threshold_similar = 0.8#.875;  #TODO: Try out what are the best ones for each type!
_window_similar    = 20;
_distance          = 'damerau'; #'edit'
_and_p_            = 0.0005#0.5#0.6;
_and_c_ = 0.000000001#0.0001;
_or_p_  = 0.0003#0.3#0.4;
_or_c_  = 0.000000001#0.0001;

CNT = 0; TRE = 1; PRO = 2;

WORD  = re.compile(r'(\b[^\s]+\b)');
CHAR  = re.compile(r'([A-Za-z]|ß|ö|ü|ä)+');
LEGAL = regex.compile(r'\p{L}+')

def ngrams(seq,n):
    return [tuple(seq[i-n:i]) for i in range(n,len(seq)+1) ];

def probs_leq(p,c,tree):
    return [(w,w_) for w in tree for w_ in tree[w][TRE] if tree[w][TRE][w_][PRO]>=p and tree[w][PRO]>=c];

def make_tree(d):
    tree = dict();
    for ngram in d:
        if len(ngram) == 2:
            if ngram[0] in tree:
                tree[ngram[0]][CNT] += d[ngram];
                if ngram[1] in tree[ngram[0]][TRE]:
                    tree[ngram[0]][TRE][ngram[1]][CNT] += d[ngram];
                else:
                    tree[ngram[0]][TRE][ngram[1]] = [d[ngram],dict(),0];
            else:
                tree[ngram[0]] = [d[ngram],{ngram[1]:[d[ngram],dict(),0]},0];
    divisor = float(sum([tree[w][CNT] for w in tree]));
    for w in tree:
        tree[w][PRO] = tree[w][CNT] / divisor;
        for w_ in tree[w][TRE]:
            tree[w][TRE][w_][PRO] = float(tree[w][TRE][w_][CNT]) / tree[w][CNT];
    return tree;

def combine_counters(counters):
    c = Counter();
    i = 0;
    for counter in counters:
        i += 1;
        if i % 50 == 0:
            print(i);
        for term in counter:
            c[term] += counter[term];
    return c;

def display(p,c,tree,inversed=False):
    if inversed:
        for w,w_ in probs_leq(p,c,tree):
            print('(inv)', w_,w, tree[w][CNT], tree[w][TRE][w_][CNT], tree[w][TRE][w_][PRO]);
    else:
        for w,w_ in probs_leq(p,c,tree):
            print('(std)', w,w_, tree[w][CNT], tree[w][TRE][w_][CNT], tree[w][TRE][w_][PRO]);

def transitive_closure(M): # WARNING: Not for large M!
    labels  = connected_components(M)[1];
    closure = csr(labels==labels[:,None]);
    return closure;

def apply_replace(index2term,replacements):
    if len(replacements) == 0:
        return dict();
    term2index     = {index2term[i]:i for i in range(len(index2term))};
    rows,cols,sims = zip(*replacements);
    R              = csr((np.ones(2*len(rows)),(rows+cols,cols+rows)),dtype=bool,shape=(len(index2term),len(index2term)));
    labels         = connected_components(R)[1];
    sorting        = np.argsort(labels);
    labels_s       = labels[sorting];
    _, starts      = np.unique(labels_s,return_index=True);
    sizes          = np.diff(starts);
    groups         = [group for group in np.split(sorting,starts[1:]) if group.size > 1];
    replace        = dict();
    for group in groups:
        terms = [index2term[i] for i in group];
        repre = max([(d[(term,)],term) for term in terms])[1];
        for term in terms:
            if term != repre:
                replace[term] = repre;
    return replace;

def replace_by_prefix(index2term,threshold,window):
    replacements = set([]);
    for i in range(len(index2term)-window):
        len_1 =  len(index2term[i]);
        for j in range(1,window+1):
            len_2   =  len(index2term[i+j]);
            percent = prefix_normed(index2term[i],index2term[i+j],len_1,len_2);
            if percent > threshold:
                replacements.add((i+j,i,percent,));
    return replacements;

def replace_by_similar(index2term,threshold,window,DIST,compared):
    replacements = set([]);
    manager      = MP.Manager();
    tasks        = manager.Queue();
    results      = manager.Queue();
    T            = [];
    for i in range(len(index2term)-window):
        T += [(i+j,i,index2term[i],index2term[i+j],len(index2term[i]),len(index2term[i+j]),threshold,DIST,) for j in range(1,window+1) if not (index2term[i+j],index2term[i],) in compared];
        if len(T) > _batch2:
            compared |= set([(index2term[ij],index2term[i],) for ij,i,term_i,term_ij,len_1,len_2,threshold,DIST in T]);
            tasks.put(T);
            T = [];
    if len(T) != 0:
        tasks.put(T);
    workers = [MP.Process(target=get_similarity_normed,args=(tasks,results,x,)) for x in range(_jobs2)];
    for worker in workers:
        worker.start();
    for x in range(_jobs2):
        result = results.get();
        replacements |= result;
        #print 'Got result', x;
    for x in range(len(workers)):
        workers[x].join();
        #print 'Joined worker', x;
    return replacements, compared;

def get_similarity_normed(tasks,results,x):
    replacements = set([]);
    while True:
        print(x,'says: Approximate number of jobs in queue:', tasks.qsize());
        try:
            T = tasks.get(timeout=3);
            #print x,'says: Got', len(T), 'tasks to do...';
        except:
            break;
        for ij,i,term_i,term_ij,len_1,len_2,threshold,DIST in T:
            percent = similarity_normed(term_i,term_ij,len_1,len_2,DIST);
            if percent > threshold:
                replacements.add((ij,i,percent,));
        #print x,'says: Done with this set of tasks.';
    #print 'Closing job', x;
    results.put(replacements);
    return 0;

def prefix_normed(term1,term2,len_1,len_2):
    prefix  =  os.path.commonprefix([term1,term2]);
    is_prefix = len(prefix)==min([len_1,len_2]);
    return float(len(prefix))/max([len_1,len_2]) if is_prefix else 0.0;

def is_prefix(term1,term2):
    return term1 == term2[:len(term1)];

def make_affixes(terms,lookback=10000):
    prefixed = {terms[0]:[(terms[0],0,)]};
    for i in range(len(terms)-1):
        affixes = [];
        pointer = 0;
        for prefix,interval in prefixed[terms[i]]: # and len(terms[i+1])-(pointer+len(prefix)) > 1
            if len(prefix) >= 2 and interval < lookback and is_prefix(prefix,terms[i+1][pointer:]): # reusing previous prefix
                affixes += [(prefix,interval+1,)];
                pointer += len(prefix);
            else:
                break;
        affixes             += [(terms[i+1][pointer:],0,)]; # new prefix
        prefixed[terms[i+1]] = affixes;
    return prefixed;

def similarity_normed(term1,term2,len_1,len_2,DIST):
    distance = damerau_dist(term1,term2) if DIST=='damerau' else edit_dist(term1,term2);
    return 1.-(float(distance)/max([len_1,len_2]));

def edit_dist(s1,s2):
    if len(s1) > len(s2):
        s1, s2 = s2, s1;
    distances = list(range(len(s1) + 1));
    for i2, c2 in enumerate(s2):
        distances_ = [i2+1];
        for i1, c1 in enumerate(s1):
            if c1 == c2:
                distances_.append(distances[i1]);
            else:
                distances_.append(1 + min((distances[i1], distances[i1 + 1], distances_[-1])));
        distances = distances_;
    return distances[-1];

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
    return thisrow[len(s2)-1];

def all_partitions(seq):
    for cutpoints in range(1 << (len(seq)-1)):
        result = []
        lastcut = 0
        for i in range(len(seq)-1):
            if (1<<i) & cutpoints != 0:
                result.append(seq[lastcut:(i+1)])
                lastcut = i+1
        result.append(seq[lastcut:])
        yield result

def conflate(affixes,d,tree):
    max_score = 0;
    max_part  = None;
    for partitioning in all_partitions(affixes):
        summ = 0;
        prob = 1;
        for j in range(len(partitioning)):
            affix = ''.join(partitioning[j]);
            prob *= -np.log(tree[partitioning[j][-1]][1][partitioning[j+1][0]][2]) if j+1 < len(partitioning) else 1;#tree[partitioning[j][-1]][2];
            summ += d[(affix,)]**len(affix);
        score = summ / prob;
        if score > max_score:
            max_score = score;
            max_part  = [''.join(partition) for partition in partitioning];
    return max_part;

def get_replacements(index2term,term2index):
    term2index_   = copy(term2index);
    replace       = dict();
    sim_prefix    = dict();
    sim_similar   = dict();
    num_replace   = 99;
    compared      = set();
    while num_replace > 0:
        print(1);
        replace_prefix  = replace_by_prefix( terms,_threshold_prefix,_window_prefix);
        print(2);
        replace_similar, compared = replace_by_similar(terms,_threshold_similar,_window_similar,_distance,compared);
        #replace_edit    = replace_by_similar(terms,_threshold_similar,_window_similar,'edit');
        print(3);
        replace_new     = apply_replace(terms,replace_prefix|replace_similar);
        print(4);
        num_replace     = len(replace_new);
        print(5);
        only_prefix     = [(terms[pair[0]],terms[pair[1]],) for pair in replace_prefix-replace_similar];
        #only_damerau    = [(terms[pair[0]],terms[pair[1]],) for pair in replace_similar-replace_edit];
        print(6);
        replace.update(replace_new);
        print(7);
        terms       = sorted(list(set([replace[term] if term in replace else term for term in terms])));
        print(8);
        term2index_ = {terms[i]:i for i in range(len(terms))};
        print(num_replace, '(',len(only_prefix),len(compared),')');#, '(',len(only_damerau),')';
    return replace;

def make_phrases(tree,tree_inv):
    #-------------------------------------------------------------------------------------
    #-right-min-left-min------------------------------------------------------------------
    set_std   = set(probs_leq(_and_p_,_and_c_,tree));
    set_inv   = set([tuple(reversed(el)) for el in probs_leq(_and_p_,_and_c_,tree_inv)]);
    inter     = set_std & set_inv;
    #-------------------------------------------------------------------------------------
    #-right-certain-left-min--------------------------------------------------------------
    set_std_  = set(probs_leq(1.0,_or_c_,tree));
    set_inv_  = set([tuple(reversed(el)) for el in probs_leq(_or_p_,_or_c_,tree_inv)]);
    inter_    = set_std_ & set_inv_;
    #-left-certain-right-min--------------------------------------------------------------
    #-------------------------------------------------------------------------------------
    set_std__ = set(probs_leq(_or_p_,_or_c_,tree));
    set_inv__ = set([tuple(reversed(el)) for el in probs_leq(1.0,_or_c_,tree_inv)]);
    inter__   = set_std__ & set_inv__;
    #-------------------------------------------------------------------------------------
    union     = inter | inter_ | inter__;
    #-------------------------------------------------------------------------------------
    #print(union);
    #print(len(inter), '+', len(inter_), '->', len(inter|inter_), '+', len(inter__), '->', len(union));
    return inter, inter_, inter__, union;

def merge_affixes(affixes_of,inter,inter_,inter__,union):
    affixes_of_ = dict();
    count = 0;
    for term in affixes_of:
        count += 1;
        if count % 1000000 == 0:
            print(count);#break;
        affixes_of_[term] = [];
        current           = affixes_of[term][0][0];
        for i in range(len(affixes_of[term])):
            if i == len(affixes_of[term])-1:
                 affixes_of_[term] += [current];
            elif not (affixes_of[term][i][0],affixes_of[term][i+1][0],) in union:
                current += affixes_of[term][i+1][0];
            else:
                affixes_of_[term] += [current];
                current            = affixes_of[term][i+1][0];
        #print(affixes_of_[term]);
    return affixes_of_;

def combine_affixes(a,b): #Unfortunately, the result makes no sense either
    a = copy(a); b = copy(b);
    for i in range(len(a)):
        if a[i].endswith('-'):
            a[i] = a[i][:-1];
    for i in range(len(b)):
        if b[i].endswith('-'):
            b[i] = b[i][:-1];
    if len(a[0]) > len(b[0]):
        z  = a; a = b; b = z;
    c = [''];
    while len(a) > 0 and len(b) > 0:
        if len(a[0]) > len(b[0]):
            z  = a; a = b; b = z;      #switch
            c     += [a[0]];           #append shorter
            #c[-1] += a[0];
            b[0]   = b[0][len(a[0]):]; #shorten longer
            a      = a[1:];            #remove shorter
            c += [''];                 #start new
        else:
            c[-1] += a[0];             #concatenate shorter
            b[0]   = b[0][len(a[0]):]; #shorten longer
            a      = a[1:];            #remove shorter
    return [el for el in c if el != ''];

def show_affixes(term,terms,term2index,affixes_of,affixes_of_inv): #TODO: Not right yet and consider: alt lakonisches vs. altlakonisc hes
    for term in terms[term2index[term]-4:term2index[term]+5]:
        a = [affix[0]       for affix in affixes_of    [term      ]];
        b = [affix[0][::-1] for affix in affixes_of_inv[term[::-1]]][::-1];
        c = combine_affixes(a,b);
        print(' '.join(a)); print(' '.join(b)); print(' '.join(c));
        print('.................................................');

def sort_terms(terms):
    fmodel     = fasttext.load_model(_model);
    lang2terms = defaultdict(list);
    for i in range(len(terms)):
        lang2terms[fmodel.predict(terms[i])[0][0][9:]].append(terms[i]);
        if i % 500000 == 0:
            print(i);
    return lang2terms;

IN = open(_termfile);
terms = [line.rstrip().strip() for line in IN];
IN.close();
index2term = copy(terms);
term2index = {terms[i]:i for i in range(len(terms))};
lang2terms = sort_terms(terms);
lerm2index = {lang:{lang2terms[lang][i]:i for i in range(len(lang2terms[lang]))} for lang in lang2terms};

input('Press ENTER to continue...')

affixes_of = {lang:dict() for lang in lang2terms};
for lang in lang2terms:
    affixes_of[lang] = make_affixes(lang2terms[lang],10000);

input('Press ENTER to continue...')

affixes_of_inv = {lang:dict() for lang in lang2terms};
for lang in lang2terms:
    affixes_of_inv[lang] = make_affixes(sorted([term[::-1] for term in lang2terms[lang]]),10000);

input('Press ENTER to continue...')

agrams   = ([(affixes_of[term][i][0],affixes_of[term][i+1][0],) for i in range(len(affixes_of[term])-1)]+[(affixes_of[term][-1][0],None,)] if len(affixes_of[term])>=2 else [(affixes_of[term][-1][0],None,)] for term in affixes_of);
agrams   = [gram for grams in agrams for gram in grams];
zgrams   = ([(affixes_of[term][i+1][0],affixes_of[term][i][0],) for i in range(len(affixes_of[term])-1)]+[(affixes_of[term][0][0],None,)] if len(affixes_of[term])>=2 else [(affixes_of[term][0][0],None,)] for term in affixes_of);
zgrams   = [gram for grams in zgrams for gram in grams];

input('Press ENTER to continue...')

tree     = make_tree(Counter(agrams));
tree_inv = make_tree(Counter(zgrams));

input('Press ENTER to continue...')

inter, inter_, inter__, union = make_phrases(tree,tree_inv);

input('Press ENTER to continue...')

affixes_of_ = merge_affixes(affixes_of,inter,inter_,inter__,union);

input('Press ENTER to continue...')

for term in terms[2879500:2879550]:
    print(' '.join((affix[0] for affix in affixes_of [term])));
    print(' '.join((affix    for affix in affixes_of_[term])));
    print('................................................');


