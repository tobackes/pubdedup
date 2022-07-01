import sys
import sqlite3

infile   = sys.argv[1];
dbfile   = sys.argv[2];
featfile = sys.argv[3];
outfile  = sys.argv[4];

alphabet = set('abcdefghijklmnopqrstuvwxyzöäü');

IN       = open(infile);
OUT      = open('2_'+outfile,'w');
OUT_more = open(outfile,'w');
OUT_many = open('many_'+outfile,'w');
con_db   = sqlite3.connect(dbfile);
cur_db   = con_db.cursor();
con_feat = sqlite3.connect(featfile);
cur_feat = con_feat.cursor();

alternatives = { 'a1sur':   ['a2sur'  ,'a3sur'  ,'a4sur'],
                 'a2sur':   ['a1sur'  ,'a3sur'  ,'a4sur'],
                 'a3sur':   ['a1sur'  ,'a2sur'  ,'a4sur'],
                 'a4sur':   ['a1sur'  ,'a2sur'  ,'a3sur'],
                 'a1init':  ['a2init' ,'a3init' ,'a4init'],
                 'a2init':  ['a1init' ,'a3init' ,'a4init'],
                 'a3init':  ['a1init' ,'a2init' ,'a4init'],
                 'a4init':  ['a1init' ,'a2init' ,'a3init'],
                 'a1first': ['a2first','a3first','a4first'],
                 'a2first': ['a1first','a3first','a4first'],
                 'a3first': ['a1first','a2first','a4first'],
                 'a4first': ['a1first','a2first','a3first'],
                 'term1':   ['term2'  ,'term3'  ,'term4'],
                 'term2':   ['term1'  ,'term3'  ,'term4'],
                 'term3':   ['term1'  ,'term2'  ,'term4'],
                 'term4':   ['term1'  ,'term2'  ,'term3'],
                 'year':    [] };

i, j = 0, 0;
for line in IN:
    i += 1;
    if i % 100000 == 0:
        print(j,'of',i);
        break; # Shortcut!
    mentionIDIndices = [int(mentionIDIndex) for mentionIDIndex in line.rstrip().split()];
    if len(mentionIDIndices) > 1000:
        print(len(mentionIDIndices), '...too long for realistic duplicate set (more than '+str(1000)+').');
        continue;
    queries = [];
    IDs     = set([]);
    for mentionIDIndex in mentionIDIndices:
        mentionID = cur_feat.execute("SELECT mentionID FROM index2mentionID WHERE mentionIDIndex=?",(mentionIDIndex,)).fetchall()[0][0];
        row       = cur_db.execute(  "SELECT * FROM publications WHERE mentionID=?",(mentionID,)).fetchall()[0];
        _,wos_id,ID,freq,title,year,a1sur,a1init,a1first,a1firstonly,a2sur,a2init,a2first,a2firstonly,a3sur,a3init,a3first,a3firstonly,a4sur,a4init,a4first,a4firstonly,term1,term2,term3,term4 = row;
        pairs     = [('year' ,year ,),
                     ('a1sur',a1sur,),('a1init',a1init,),('a1first',a1first,),
                     ('a2sur',a2sur,),('a2init',a2init,),('a2first',a2first,),
                     ('a3sur',a3sur,),('a3init',a3init,),('a3first',a3first,),
                     ('a4sur',a4sur,),('a4init',a4init,),('a4first',a4first,),
                     ('term1',term1,),('term2' ,term2 ,),('term3'  ,term3  ,),('term4',term4,)];
        pairs     = [pair for pair in pairs if (pair[0]!='a1sur'  or a1init ==None) and (pair[0]!='a2sur'  or a2init ==None) and (pair[0]!='a3sur'  or a3init ==None) and (pair[0]!='a4sur'  or a4init ==None)];
        pairs     = [pair for pair in pairs if (pair[0]!='a1init' or a1first==None) and (pair[0]!='a2init' or a2first==None) and (pair[0]!='a3init' or a3first==None) and (pair[0]!='a4init' or a4first==None)];
        queries.append('('+' AND '.join(['('+' OR '.join(field_+'='+[str(value),'"'+str(value)+'"'][isinstance(value,str)] for field_ in [field]+alternatives[field])+')' for field,value in pairs if value!=None])+')');
        if ID != None:
            IDs.add(ID);
    if len(IDs) == 0:
        continue; # THIS IS A SIMPLIFICATION FOR EVALUATION PURPOSES! RETHINK! Also this only checks whether there are IDs in the minels
    j += 1;
    query = "SELECT mentionID FROM publications WHERE " + " OR ".join(queries);
    if len(queries) <= 2:
        OUT.write(query+'\n');
    elif len(queries) >= 10:
        OUT_many.write(query+'\n');
    else:
        OUT_more.write(query+'\n');

IN.close();
OUT.close();
OUT_more.close();
OUT_many.close();
con_db.close();
con_feat.close();
