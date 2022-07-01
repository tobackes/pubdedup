import sys
import sqlite3

_infile  = sys.argv[1];
_outfile = sys.argv[2];

_min_gold_size = 2; #2

PREFIX = "\\begin{tabular}{||cc||c|c|c|c|c|c||c|c|c|c|c|c||c|c|c|c|c|c||c|c|c|c|c|c||c|c|c|c|c|c||c|c||}\n\\hhline{|t:==================================:t|}\n\\multicolumn{34}{||c||}{\\textbf{min gold size = 1}} \\\\\n\\hhline{|:==:t:======:t:======:t:======:t:======:t:======:t:==:|}\n\\multicolumn{2}{||l||}{\\textbf{max gold}} & \\multicolumn{6}{c||}{\\textbf{10}} & \\multicolumn{6}{c||}{\\textbf{25}} & \\multicolumn{6}{c||}{\\textbf{50}} & \\multicolumn{6}{c||}{\\textbf{100}} & \\multicolumn{6}{c||}{\\textbf{1000}} & \\multicolumn{2}{c||}{\\textbf{core}} \\\\\n\\multicolumn{2}{||l||}{\\textbf{max label}} & \\textbf{10\\textsuperscript{1}} & \\textbf{10\\textsuperscript{2}} & \\textbf{10\\textsuperscript{3}} & \\textbf{10\\textsuperscript{4}} & \\textbf{10\\textsuperscript{5}} & \\textbf{\\textbf{10\\textsuperscript{6}}} & \\textbf{10\\textsuperscript{1}} & \\textbf{10\\textsuperscript{2}} & \\textbf{10\\textsuperscript{3}} & \\textbf{10\\textsuperscript{4}} & \\textbf{10\\textsuperscript{5}} & \\textbf{\\textbf{\\textbf{10\\textsuperscript{6}}}} & \\textbf{10\\textsuperscript{1}} & \\textbf{10\\textsuperscript{2}} & \\textbf{10\\textsuperscript{3}} & \\textbf{10\\textsuperscript{4}} & \\textbf{10\\textsuperscript{5}} & \\textbf{\\textbf{10\\textsuperscript{6}}} & \\textbf{10\\textsuperscript{1}} & \\textbf{10\\textsuperscript{2}} & \\textbf{10\\textsuperscript{3}} & \\textbf{10\\textsuperscript{4}} & \\textbf{10\\textsuperscript{5}} & \\textbf{\\textbf{10\\textsuperscript{6}}} & \\textbf{10\\textsuperscript{1}} & \\textbf{10\\textsuperscript{2}} & \\textbf{10\\textsuperscript{3}} & \\textbf{10\\textsuperscript{4}} & \\textbf{10\\textsuperscript{5}} & \\textbf{\\textbf{10\\textsuperscript{6}}} & \\textbf{ du} & \\textbf{ no} \\\\\n\\hhline{|:==::======::======::======::======::======::==:|}\n\n";
INFIX  = "\\multirow{2}{*}{\\textbf{#METHOD#}} & \\textbf{P} & 000 & 010 & 020 & 030 & 040 & 050 & 100 & 110 & 120 & 130 & 140 & 150 & 200 & 210 & 220 & 230 & 240 & 250 & 300 & 310 & 320 & 330 & 340 & 350 & 400 & 410 & 420 & 430 & 440 & 450 & core_pos0 & core_neg0 \\\\\n & \\textbf{R} & 001 & 011 & 021 & 031 & 041 & 051 & 101 & 111 & 121 & 131 & 141 & 151 & 201 & 211 & 221 & 231 & 241 & 251 & 301 & 311 & 321 & 331 & 341 & 351 & 401 & 411 & 421 & 431 & 441 & 451 & core_pos1 & core_neg1 \\\\\n\n";
JOINT  = "\hhline{|:==::======::======::======::======::======::==:|}\n\n";
SUFFIX = "\\hhline{|b:==:b:======:b:======:b:======:b:======:b:======:b:==:b|}\n\\end{tabular}";

_maxgold2index  = { 10:0,  25:1,   50:2,   100:3,   1000:4            };
_maxlabel2index = { 10:0, 100:1, 1000:2, 10000:3, 100000:4, 1000000:5 };
_repres2index   = { 'words':1, 'bigrams':2, 'ngrams':3, 'title':4 };

_specif2index  = { 'restrictions_publications_words':                     1,
                   'restrictions_publications_words_stricter':            2,
                   'restrictions_publications_words_mentions':            3,
                   'restrictions_publications_words_stricter_mentions':   4,
                   'restrictions_publications_bigrams':                   1,
                   'restrictions_publications_bigrams_stricter':          2,
                   'restrictions_publications_bigrams_mentions':          3,
                   'restrictions_publications_bigrams_stricter_mentions': 4,
                   'restrictions_publications_ngrams':                    1,
                   'restrictions_publications_ngrams_mentions':           2,
                   'restrictions_publications_title':                     1 };

#_specif2index   = { 'restrictions_publications_stricter_mentions':1 };
#_genera2index   = { '8281_3021_4441_0_40':1, '6241_3021_4441_0_20':2, '4261_3021_4441_0_20':3, 'no_generalization':4 };
_genera2index   = { '8281_3021_4441_0_20':     1,
                    '6241_3021_4441_0_20_sur': 2,
                    '6241_3021_4441_0_20':     3,
                    '4261_3021_4441_0_20':     4,
                    'no_generalization':       5,
                    '8281_3021_4441_0_40':     6 };
#_genera2index   = { '4261_3021_4441_0_20':1 };

def get_handle(representation,specification,generalization):
    print(str(_repres2index[representation]),str(_specif2index[specification]),str(_genera2index[generalization]));
    print('[P'+str(_repres2index[representation])+str(_specif2index[specification])+str(_genera2index[generalization])+']');
    return '[P'+str(_repres2index[representation])+str(_specif2index[specification])+str(_genera2index[generalization])+']';

def insert(cur,min_gold_size,representation,specification,generalization,infix): #TODO: Test this with the right generalization
    rows   = cur.execute('SELECT max_gold_size, max_label_size, label_prec, label_rec FROM results WHERE representation=? AND specification=? AND generalization=? AND min_gold_size=? AND max_gold_size < 10000 AND dataset!="mentions_core"',(representation,specification,generalization,min_gold_size,)).fetchall();
    for max_gold_size, max_label_size, label_prec, label_rec in rows:
        locator = str(_maxgold2index[max_gold_size])+str(_maxlabel2index[max_label_size]);
        infix  = infix.replace(locator+'0',str(round(label_prec)));
        infix  = infix.replace(locator+'1',str(round(label_rec )));
    rows   = cur.execute('SELECT eval_mode, label_prec, label_rec FROM results WHERE representation=? AND specification=? AND generalization=? AND dataset="mentions_core"',(representation,specification,generalization,)).fetchall();
    for eval_mode, label_prec, label_rec in rows:
        if eval_mode != 'pair':
            locator = 'core_pos' if eval_mode == 'core' else 'core_neg' if eval_mode == 'core_neg' else None;
            infix = infix.replace(locator+'0',str(round(label_prec)));
            infix = infix.replace(locator+'1',str(round(label_rec )));
    infix = infix.replace('#METHOD#',get_handle(representation,specification,generalization));
    return infix;


con = sqlite3.connect(_infile);
cur = con.cursor();

rows = cur.execute("SELECT DISTINCT representation,specification,generalization FROM results WHERE min_gold_size=? AND max_gold_size < 10000",(_min_gold_size,)).fetchall();

infix = JOINT.join([insert(cur,_min_gold_size,representation,specification,generalization,INFIX) for representation,specification,generalization in rows]);
tex   = PREFIX.replace('#MINGOLD#',str(_min_gold_size)) + infix + SUFFIX;

con.close();

OUT = open(_outfile,'w');
OUT.write(tex);
OUT.close();
