import sys
import sqlite3

infile   = sys.argv[1];
dbfile   = sys.argv[2];
featfile = sys.argv[3];

alphabet = set('abcdefghijklmnopqrstuvwxyzöäü');

IN       = open(infile);
con_db   = sqlite3.connect(dbfile);
cur_db   = con_db.cursor();
con_feat = sqlite3.connect(featfile);
cur_feat = con_feat.cursor();

for line in IN:
    print('------------------------------------------');
    mentionIDIndices = [int(mentionIDIndex) for mentionIDIndex in line.rstrip().split()];
    if len(mentionIDIndices) > 100:
        print(len(mentionIDIndices), '...too long for realistic duplicate set (more than 20).');
        continue;
    skip = False;
    mentionIDs = [];
    for mentionIDIndex in mentionIDIndices:
        mentionID = cur_feat.execute("SELECT mentionID FROM index2mentionID WHERE mentionIDIndex=?",(mentionIDIndex,)).fetchall()[0][0];
        title     = cur_db.execute("SELECT title FROM publications WHERE mentionID=?",(mentionID,)).fetchall()[0][0];
        feats     = [row[0] for row in cur_feat.execute("SELECT feat FROM index2feat WHERE featIndex IN (SELECT featIndex FROM features WHERE mentionIDIndex=?) AND feat IS NOT NULL AND feat IS NOT ' '",(mentionIDIndex,))];
        if len(feats[0])==0 or not feats[0][0] in alphabet:
            print('...non-alphabetical character.'); skip=True;
            break;
        print(str(mentionID)+' : '+str(title)+': '+' | '.join(sorted(feats)));
        mentionIDs.append(mentionID);
    print('------------------------------------------');
    print('SELECT mentionID FROM publications WHERE mentionID IN ("'+'","'.join(mentionIDs)+'")')
    print('------------------------------------------');
    if not skip: input('Enter to continue...');

IN.close();
con_db.close();
con_feat.close();
