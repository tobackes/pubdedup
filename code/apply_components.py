import sqlite3
import sys

_compDB = sys.argv[1];
_featDB = sys.argv[2];
_reprDB = sys.argv[3];
_outDB  = sys.argv[4];

_batch = 100000;


def translate(label2repIDIndex):
    for label, repIDIndex in label2repIDIndex:
        repID = repIDIndex2repID[repIDIndex];
        if repID in repID2mentionIDIndex: # With generalizations there might not be a mentionIDIndex for a representation?
            mentionIDIndex = repID2mentionIDIndex[repID];
            mentionID      = mentionIDIndex2mentionID[mentionIDIndex];
            yield (label,mentionID,);

print('Creating mapping repIDIndex --> repID...');
con = sqlite3.connect(_featDB);
cur = con.cursor();
cur.execute("SELECT repIDIndex, repID FROM index2repID");
repIDIndex2repID = {repIDIndex:repID for repIDIndex, repID in cur};
con.close();

print('Creating mapping repID --> mentionIDIndex...');
con = sqlite3.connect(_reprDB);
cur = con.cursor();
cur.execute("SELECT repID, mentionIDIndex FROM mention2repID");
repID2mentionIDIndex = {repID:mentionIDIndex for repID, mentionIDIndex in cur};

print('Creating mapping mentionIDIndex --> mentionID...');
cur.execute("SELECT mentionIDIndex, mentionID FROM index2mentionID");
mentionIDIndex2mentionID = {mentionIDIndex:mentionID for mentionIDIndex, mentionID in cur};
con.close();

con_out = sqlite3.connect(_outDB);
cur_out = con_out.cursor();

print('Creating target  label --> mentionID...');
cur_out.execute("DROP TABLE IF EXISTS label2mentionID");
cur_out.execute("CREATE TABLE label2mentionID(label INT, mentionID TEXT PRIMARY KEY)");
con = sqlite3.connect(_compDB);
cur = con.cursor();
cur.execute("SELECT label, repIDIndex FROM components");
while True:
    label2repIDIndex = cur.fetchmany(_batch);
    if len(label2repIDIndex) == 0:
        break;
    cur_out.executemany("INSERT INTO label2mentionID VALUES(?,?)",translate(label2repIDIndex));
    con_out.commit();

print('Creating target  label --> minel...');
cur_out.execute("DROP TABLE IF EXISTS minel2label");
cur_out.execute("CREATE TABLE minel2label(minel INT, label INT)");
cur.execute("SELECT label, minel FROM minel2label");
while True:
    label2minel = cur.fetchmany(_batch);
    if len(label2minel) == 0:
        break;
    cur_out.executemany("INSERT INTO minel2label(label,minel) VALUES(?,?)",translate(label2minel));
    con_out.commit();

print('Creating target  mentionIDIndex --> minel...');
cur_out.execute("DROP TABLE IF EXISTS mentionIDIndex2minel");
cur_out.execute("CREATE TABLE mentionIDIndex2minel(mentionIDIndex INT, minel INT)");
cur.execute("SELECT repIDIndex, minel FROM repIDIndex2minel");
while True:
    repIDIndex2minel = cur.fetchmany(_batch);
    if len(repIDIndex2minel) == 0:
        break;
    cur_out.executemany("INSERT INTO mentionIDIndex2minel(minel,mentionIDIndex) VALUES(?,?)",translate(((label,repIDIndex,) for repIDIndex,label in translate(repIDIndex2minel))));
    con_out.commit();

con_out.close();
con.close();
