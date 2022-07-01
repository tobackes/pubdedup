import sys
import sqlite3
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt


_infolder  = sys.argv[1];
_outfolder = sys.argv[2];
_method    = sys.argv[3];

def count_feats(label,cur):
    cur.execute("SELECT featGroup||'##:##'||feat FROM feats.index2feat WHERE featIndex IN (SELECT featIndex FROM feats.features WHERE repIDIndex IN (SELECT repIDIndex FROM components WHERE label="+str(label)+"));");
    features = [row[0] for row in cur.fetchall()];
    grouped  = dict();
    for feature in features:
        group,feat = feature.split('##:##');
        if group in grouped:
            grouped[group].append(feat);
        else:
            grouped[group] = [feat];
    return Counter(features), {group:Counter(grouped[group]) for group in grouped};

def top_k_components(k,cur):
    cur.execute("SELECT label FROM (SELECT COUNT(*) AS freq,label FROM components GROUP BY label) ORDER BY freq DESC");
    rows = cur.fetchmany(k);
    rest = cur.fetchall();
    return [row[0] for row in rows];

_con = sqlite3.connect(_infolder+'components_'+_method+'.db');
_cur = _con.cursor();

_cur.execute('ATTACH DATABASE "'+_infolder+'features.db" AS feats');
_cur.execute('ATTACH DATABASE "'+_infolder+'representations.db" AS reps');

print('Getting top-k components by size...');
labels = top_k_components(25,_cur);

print('Plotting wordclouds...');
for i in range(len(labels)):
    counts, grouped = count_feats(labels[i],_cur);
    wordcloud       = WordCloud(background_color='white');
    wordcloud.generate_from_frequencies(frequencies=counts);
    plt.figure();
    plt.imshow(wordcloud, interpolation="bilinear");
    plt.axis("off");
    plt.savefig(_outfolder+str(i)+'_'+str(labels[i])+'.png',dpi=300);
    for group in grouped:
        wordcloud = WordCloud(background_color='white');
        wordcloud.generate_from_frequencies(frequencies=grouped[group]);
        plt.figure();
        plt.imshow(wordcloud, interpolation="bilinear");
        plt.axis("off");
        plt.savefig(_outfolder+str(i)+'_'+str(labels[i])+'_'+group+'.png',dpi=300);

_con.close();
