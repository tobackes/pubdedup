import sqlite3
import sys
from collections import defaultdict

_mentDB = sys.argv[1];
_compDB = sys.argv[2];
_featDB = sys.argv[3];
_reprDB = sys.argv[4];
_outDB  = sys.argv[5];

_type = 'representations';

_batch = 100000;


def translate(label2repIDIndex,repIDIndex2repID,repID2mentionIDIndeces,mentionIDIndex2mentionID):
    for label, repIDIndex in label2repIDIndex:
        repID = repIDIndex2repID[repIDIndex];
        if repID in repID2mentionIDIndeces: # With generalizations there might not be a mentionIDIndex for a representation?
            for mentionIDIndex in repID2mentionIDIndeces[repID]:
                mentionID = mentionIDIndex2mentionID[mentionIDIndex];
                yield (label,mentionID,);


print('Creating mapping mentionID --> id...');
con = sqlite3.connect(_mentDB);
cur = con.cursor();
cur.execute("SELECT mentionID, id FROM "+_type);
mentionID2id = {mentionID:ID for mentionID, ID in cur};
con.close();

print('Creating mapping repIDIndex --> repID...');
con       = sqlite3.connect(_featDB);
cur       = con.cursor();
max_index = cur.execute("SELECT max(repIDIndex) FROM index2repID").fetchall()[0][0];
cur.execute("SELECT repIDIndex, repID FROM index2repID");
repIDIndex2repID = [None for i in range(max_index)];
for repIDIndex, repID in cur:
    repIDIndex2repID[repIDIndex] = repID;
#repIDIndex2repID = {repIDIndex:repID for repIDIndex, repID in cur};
con.close();

print('Creating mapping repID --> mentionIDIndeces...');
con = sqlite3.connect(_reprDB);
cur = con.cursor();
cur.execute("SELECT repID, mentionIDIndex FROM mention2repID");
repID2mentionIDIndeces = defaultdict(set);
for repID, mentionIDIndex in cur:
    repID2mentionIDIndeces[repID].add(mentionIDIndex);

print('Creating mapping mentionIDIndex --> mentionID...');
max_index = cur.execute("SELECT max(mentionIDIndex) FROM index2mentionID").fetchall()[0][0];
cur.execute("SELECT mentionIDIndex, mentionID FROM index2mentionID");
mentionIDIndex2mentionID = [None for i in range(max_index)];
for mentionIDIndex, mentionID in cur:
    mentionIDIndex2mentionID[mentionIDIndex] = mentionID;
#mentionIDIndex2mentionID = {mentionIDIndex:mentionID for mentionIDIndex, mentionID in cur};
con.close();

con = sqlite3.connect(_compDB);
cur = con.cursor();

print('Creating mapping label --> repIDIndeces...');
label2repIDIndeces = defaultdict(set);
cur.execute("SELECT label, repIDIndex FROM components");
for label, repIDIndex in cur:
    label2repIDIndeces[label].add(repIDIndex);

con_out = sqlite3.connect(_outDB);
cur_out = con_out.cursor();

print('Creating target  label --> mentionID...');
cur_out.execute("DROP TABLE IF EXISTS label2mentionID");
cur_out.execute("CREATE TABLE label2mentionID(label INT, mentionID TEXT PRIMARY KEY)");
#cur.execute("SELECT label, repIDIndex FROM components");

label3repIDIndex = ((label,repIDIndex,) for label in label2repIDIndeces for repIDIndex in label2repIDIndeces[label]);
cur_out.executemany("INSERT INTO label2mentionID VALUES(?,?)",translate(label3repIDIndex,repIDIndex2repID,repID2mentionIDIndeces,mentionIDIndex2mentionID));
con_out.commit();

print('Creating target  label --> minel...');
cur_out.execute("DROP TABLE IF EXISTS minel2label");
cur_out.execute("CREATE TABLE minel2label(minel INT, label INT)");
cur.execute("SELECT label, minel FROM minel2label");
while True:
    label2minel = cur.fetchmany(_batch);
    if len(label2minel) == 0:
        break;
    cur_out.executemany("INSERT INTO minel2label(label,minel) VALUES(?,?)",translate(label2minel,repIDIndex2repID,repID2mentionIDIndeces,mentionIDIndex2mentionID));
    con_out.commit();

print('Creating target  mentionIDIndex --> minel...');
cur_out.execute("DROP TABLE IF EXISTS mentionIDIndex2minel");
cur_out.execute("CREATE TABLE mentionIDIndex2minel(mentionIDIndex INT, minel INT)");
cur.execute("SELECT repIDIndex, minel FROM repIDIndex2minel");
while True:
    repIDIndex2minel = cur.fetchmany(_batch);
    if len(repIDIndex2minel) == 0:
        break;
    cur_out.executemany("INSERT INTO mentionIDIndex2minel(minel,mentionIDIndex) VALUES(?,?)",translate(((label,repIDIndex,) for repIDIndex,label in translate(repIDIndex2minel,repIDIndex2repID,repID2mentionIDIndeces,mentionIDIndex2mentionID)),repIDIndex2repID,repID2mentionIDIndeces,mentionIDIndex2mentionID));
    con_out.commit();

print('Creating target  mentions...');
cur_out.execute("DROP TABLE IF EXISTS mentions");
cur_out.execute("CREATE TABLE mentions(mentionID INTEGER PRIMARY KEY, repID TEXT, id INT, label INT)");
#for label in label2repIDIndex:
#    repID = repIDIndex2repID[repIDIndex];
#    for mentionIDIndex in repID2mentionIDIndeces[repID]:
#        mentionID = mentionIDIndex2mentionID[mentionIDIndex];
#        ID        = mentionID2id[mentionID];
generator = ((mentionIDIndex2mentionID[mentionIDIndex],repIDIndex2repID[repIDIndex],mentionID2id[mentionIDIndex2mentionID[mentionIDIndex]],label,) for label in label2repIDIndeces for repIDIndex in label2repIDIndeces[label] for mentionIDIndex in repID2mentionIDIndeces[repIDIndex2repID[repIDIndex]]);
cur_out.executemany("INSERT INTO mentions VALUES(?,?,?,?)",generator);
con_out.commit();
cur_out.execute("CREATE INDEX mentions_repID_index on mentions(repID)");
cur_out.execute("CREATE INDEX mentions_id_index    on mentions(id)");
cur_out.execute("CREATE INDEX mentions_label_index on mentions(label)");

con_out.close();
con.close();
