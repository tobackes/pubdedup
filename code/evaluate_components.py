import sqlite3
import sys
from collections import defaultdict

_compDB = sys.argv[1];
_mentDB = sys.argv[2];
_reprDB = sys.argv[3];
_output = sys.argv[4];

con = sqlite3.connect(_compDB);
cur = con.cursor();

#TODO: Change by going over mentionIDs in mentions where id IS NOT NULL and looking up rIDIndex and the respective label

cur.execute('ATTACH DATABASE "'+_mentDB+'" AS "mentions"'       );
cur.execute('ATTACH DATABASE "'+_reprDB+'" AS "representations"');

rids    = [row[0] for row in cur.execute("SELECT DISTINCT id FROM mentions.authors WHERE id IS NOT NULL")];
midrids = cur.execute("SELECT mentionID,id FROM mentions.authors WHERE id IS NOT NULL ORDER BY id").fetchall();

rid2labels = defaultdict(list);

OUT = open(_output,'w');
OUT.close();
#TODO: This is now much slower still. Think more about it and check for bugs! Maybe just load the mappings to memory.
i = 0;
last_id = None;
for mentionID, rID in midrids:
    mentionIDIndex  = cur.execute("SELECT mentionIDIndex FROM representations.index2mentionID WHERE mentionID=?",(mentionID,)).fetchall()[0][0];
    repID           = cur.execute("SELECT DISTINCT repID FROM representations.mention2repID WHERE mentionIDIndex=?",(mentionIDIndex,)).fetchall()[0][0];
    repIDIndex      = cur.execute("SELECT repIDIndex FROM representations.index2repID WHERE repID=?",(repID,)).fetchall()[0][0];
    label_rows      = cur.execute("SELECT label FROM components WHERE repIDIndex=?",(repIDIndex,)).fetchall();
    label           = label_rows[0][0] if label_rows != [] else None;
    if label       != None:
        rid2labels[rID].append(label);
        if rID != last_id:
            last_id = rID;
            print(rID,rid2labels[rID]);
            OUT = open(_output,'a');
            OUT.write(rID+' '+','.join((str(label) for label in rid2labels[rID]))+'\n');
            OUT.close();
            i += 1;
            if i % 10 == 0:
                print(100.*i/len(midrids), '%');

'''
i = 0;
for rid in rids:
    labels = [row[0] for row in cur.execute("SELECT DISTINCT label FROM components WHERE repIDIndex IN (SELECT repIDIndex FROM representations.index2repID WHERE repID IN (SELECT DISTINCT repID FROM representations.mention2repID WHERE mentionIDIndex IN (SELECT mentionIDIndex FROM representations.index2mentionID WHERE mentionID IN (SELECT mentionID FROM mentions.authors WHERE id=?))))",(rid,)).fetchall()];
    rid2labels[rid] = labels;
    print(rid,labels);
    OUT = open(_output,'a');
    OUT.write(rid+' '+','.join((str(label) for label in labels))+'\n');
    OUT.close();
    i += 1;
    if i % 10 == 0:
        print(100.*i/len(rids), '%',end='\r');
'''
