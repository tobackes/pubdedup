import sqlite3
import numpy as np
from scipy.sparse import csr_matrix as csr
from scipy.sparse import vstack as csr_vstack
from collections import Counter

#labelled_mentions.db:mentions(mentionID PRIMARY KEY, repID TEXT, goldID INT, label INT);

def load_examples(cur):
    examples = dict();
    rows     = {size:goldID for size,goldID in cur.execute("SELECT freq,goldID FROM (SELECT COUNT(DISTINCT repID) AS freq,goldID FROM mentions WHERE goldID IS NOT NULL GROUP BY goldID) WHERE freq>1 AND freq <26 GROUP BY freq;")};
    for size in rows:
        goldID = rows[size]
        cur.execute('SELECT repIDIndex,GROUP_CONCAT(featIndex) FROM feats.features WHERE repIDIndex IN (SELECT repIDIndex FROM feats.index2repID WHERE repID IN (SELECT repID FROM mentions WHERE goldID="'+goldID+'")) GROUP BY repIDIndex');
        for repIDIndex,features in cur:
            examples[repIDIndex] = [int(featIndex) for featIndex in features.split(',')];
    return examples;

def randomvector(d,ignore): #TODO: store only the values for featIndices that are actually used
    components = np.random.standard_normal(d);
    r          = np.sqrt(np.square(components).sum());
    v          = components / r;
    v[ignore]  = 0;
    v          = csr(v,shape=(1,d));#    v.eliminate_zeros();
    return v;

con = sqlite3.connect("/data_ssds/disk11/backests/pubdedup/representations_publications/restrictions_publications/4261_3021_4441_0_20/labelled_mentions.db");
cur = con.cursor();

cur.execute('ATTACH database "/data_ssds/disk11/backests/pubdedup/representations_publications/restrictions_publications/4261_3021_4441_0_20/features.db" AS feats');

d = cur.execute("SELECT MAX(featIndex) FROM features").fetchall()[0][0];
p = 5; # With smaller p, the randomness of the h Matrix is more and more respobsible for the grouping, so records are grouped more and more by chance rather than similarity
       # It seems that the simHash method requires many features per record and features with more generality, so probably a rather small feat-vocab, our method does the opposite
       # So for proper application of simHash, one would need to sample different features, like character ngrams over the title, etc.
       # It is also likely that simHash is better at getting high precision or finding outlier pairs that have exceptionally high overlap rather than finding significant overlap with few features
       # In other words if you want to encode the significance of individual features, the significant ones need to be split into more smaller features than the insignificant

examples = load_examples(cur);

index2repIDIndex = list(examples.keys());
repIDIndex2index = {index2repIDIndex[i]:i for i in range(len(index2repIDIndex))};

rows   = [ feat                         for repIDIndex in index2repIDIndex for feat in examples[repIDIndex] ];
cols   = [ repIDIndex2index[repIDIndex] for repIDIndex in index2repIDIndex for feat in examples[repIDIndex] ];
data   = np.ones(len(cols), dtype=bool);
feats  = set(rows);
ignore = [i for i in range(d) if not i in feats];

D = csr( ( data, (rows,cols) ) , shape=(d,len(index2repIDIndex)));

h = csr_vstack([randomvector(d,ignore) for i in range(p)]);

hashes = h.dot(D).toarray() > 0;

bitstrings = [''.join(['1' if hashes[i,j] else '0' for i in range(p)]) for j in range(len(index2repIDIndex))];

duplicates = Counter(bitstrings);

labels = dict();
for j in range(len(bitstrings)):
    if bitstrings[j] in labels:
        labels[bitstrings[j]].append(index2repIDIndex[j]);
    else:
        labels[bitstrings[j]] = [index2repIDIndex[j]];

for bitstring in labels:
    repIDIndices = labels[bitstring];
    print('-----------',bitstring,'----------------------------------');
    for repIDIndex in repIDIndices:
        features = cur.execute('SELECT featGroup,feat FROM feats.index2feat WHERE featIndex in (SELECT featIndex FROM feats.features WHERE repIDIndex="'+str(repIDIndex)+'")').fetchall();
        print(repIDIndex,':',features);
    print('-----------------------------------------------------------');
