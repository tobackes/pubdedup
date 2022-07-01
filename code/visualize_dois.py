import sys
import sqlite3
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt


_mentions  = sys.argv[1];
_infolder  = sys.argv[2];
_outfolder = sys.argv[3];

def count_feats(doi,cur):
    cur.execute("SELECT featGroup||':'||feat FROM feats.index2feat WHERE featIndex IN (SELECT featIndex FROM feats.features WHERE repIDIndex IN (SELECT repIDIndex FROM feats.index2repID WHERE repID IN (SELECT DISTINCT repID FROM (SELECT repID FROM reps.mention2repID where mentionIDIndex in (SELECT mentionIDIndex FROM reps.index2mentionID WHERE mentionID IN (SELECT mentionID FROM mentions WHERE goldID=?))))))",(doi,));
    features = [row[0] for row in cur.fetchall()];
    grouped  = dict();
    for feature in features:
        group,feat = feature.split(':');
        if group in grouped:
            grouped[group].append(feat);
        else:
            grouped[group] = [feat];
    return Counter(features), {group:Counter(grouped[group]) for group in grouped};

def top_k_dois(k,cur):
    cur.execute("SELECT COUNT(*),freq,goldID FROM (SELECT COUNT(*) AS freq,goldID FROM mentions WHERE goldID IS NOT NULL GROUP BY goldID) GROUP BY freq ORDER BY freq DESC");
    rows = cur.fetchmany(k);
    rest = cur.fetchall();
    return [doi for count,freq,doi in rows];

_con = sqlite3.connect(_mentions);
_cur = _con.cursor();

_cur.execute('ATTACH DATABASE "'+_infolder+'features.db"        AS feats');
_cur.execute('ATTACH DATABASE "'+_infolder+'representations.db" AS reps');

print('Getting top-k goldIDs by size...');
dois = top_k_dois(25,_cur);

print('Plotting wordclouds...');
for i in range(len(dois)):
    counts, grouped = count_feats(dois[i],_cur);
    wordcloud       = WordCloud(background_color='white');
    wordcloud.generate_from_frequencies(frequencies=counts);
    plt.figure();
    plt.imshow(wordcloud, interpolation="bilinear");
    plt.axis("off");
    plt.savefig(_outfolder+str(i)+'.png',dpi=300);
    for group in grouped:
        wordcloud = WordCloud(background_color='white');
        wordcloud.generate_from_frequencies(frequencies=grouped[group]);
        plt.figure();
        plt.imshow(wordcloud, interpolation="bilinear");
        plt.axis("off");
        plt.savefig(_outfolder+str(i)+'_'+group+'.png',dpi=300);

_con.close();
