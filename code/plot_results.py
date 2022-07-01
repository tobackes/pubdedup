import sys
import numpy as np
import sqlite3
import matplotlib
#matplotlib.use('Agg')
from matplotlib import pyplot as plt
from matplotlib.ticker import MaxNLocator
#from matplotlib.ticker import MultipleLocator

_infile  = sys.argv[1];
_outfile = sys.argv[2] if len(sys.argv) > 2 else None;

CO  = None;
LW  = 1;
COs = ['cyan','orange','green','red','blue','magenta'];

_reps = ['words_stricter','bigrams_stricter'];
_gens = ['8281_3021_4441_0_20','6241_3021_4441_0_20','4261_3021_4441_0_20','no_generalization'];
_mentions = True;


def plot_rest(cur,representation,specification,generalization,min_gold_size,column,n_cols):
    #--------------------------------------------------------------------------------------------------------------------
    query = 'SELECT * FROM results WHERE max_gold_size<10000 AND eval_mode="pair" AND dataset="mentions" AND min_gold_size='+str(min_gold_size)+' AND representation="'+representation+'" AND specification="'+specification+'" AND generalization="'+generalization+'"';
    cur.execute(query);
    #--------------------------------------------------------------------------------------------------------------------
    index2column = [row[0] for row in cur.description];
    column2index = {index2column[i]:i for i in range(len(index2column))};
    #--------------------------------------------------------------------------------------------------------------------
    rows = cur.fetchall();
    #for row in rows:
    #    print(row);
    #--------------------------------------------------------------------------------------------------------------------
    SIZE = column2index['max_gold_size'];
    RP = column2index['rep_prec'];  RR = column2index['rep_rec'];
    MP = column2index['base_prec']; MR = column2index['base_rec'];
    #--------------------------------------------------------------------------------------------------------------------
    sizes = np.array([row[SIZE] for row in rows]);
    rps   = np.array([row[RP  ] for row in rows]);
    rrs   = np.array([row[RR  ] for row in rows]);
    mps   = np.array([row[MP  ] for row in rows]);
    mrs   = np.array([row[MR  ] for row in rows]);
    #--------------------------------------------------------------------------------------------------------------------
    ax1 = axes[0,column-1];
    #--------------------------------------------------------------------------------------------------------------------
    ax1.plot(sizes, rps, color='black', ls='--' , label='representations', linewidth=LW);
    ax1.plot(sizes, mps, color='black', ls='-.'  , label='mentions'      , linewidth=LW);
    #--------------------------------------------------------------------------------------------------------------------
    ax2 = axes[1,column-1];
    #--------------------------------------------------------------------------------------------------------------------
    ax2.plot(sizes, rrs, color='black', ls='--', label='representations', linewidth=LW);
    ax2.plot(sizes, mrs, color='black', ls='-.' , label='mentions'      , linewidth=LW);
    #--------------------------------------------------------------------------------------------------------------------
    #ax2.legend(loc='center left', bbox_to_anchor=(1.1,0.5), fontsize=7, framealpha=1, edgecolor='k');


def plot(cur,representation,specification,generalization,min_gold_size,max_label_size,column,n_cols):
    #label = representation+' / '+specification+' / '+generalization+' / '+str(max_label_size);
    label = str(max_label_size);
    #--------------------------------------------------------------------------------------------------------------------
    query = 'SELECT * FROM results WHERE max_gold_size<10000 AND eval_mode="pair" AND dataset="mentions" AND min_gold_size='+str(min_gold_size)+' AND representation="'+representation+'" AND specification="'+specification+'" AND generalization="'+generalization+'" AND max_label_size='+str(max_label_size);
    cur.execute(query);
    #--------------------------------------------------------------------------------------------------------------------
    index2column = [row[0] for row in cur.description];
    column2index = {index2column[i]:i for i in range(len(index2column))};
    #--------------------------------------------------------------------------------------------------------------------
    rows = cur.fetchall();
    #for row in rows:
    #    print(row);
    #--------------------------------------------------------------------------------------------------------------------
    SIZE = column2index['max_gold_size'];
    LP   = column2index['label_prec']; LR = column2index['label_rec']; RP = column2index['rep_prec']; RR = column2index['rep_rec'];
    #--------------------------------------------------------------------------------------------------------------------
    sizes = np.array([row[SIZE] for row in rows]);
    lps   = np.array([row[LP  ] for row in rows]);
    lrs   = np.array([row[LR  ] for row in rows]);
    rps   = np.array([row[RP  ] for row in rows]);
    rrs   = np.array([row[RR  ] for row in rows]);
    #--------------------------------------------------------------------------------------------------------------------
    ax1 = axes[0,column];
    #--------------------------------------------------------------------------------------------------------------------
    ax1.plot(sizes, lps, color=CO, ls='-' , label=label, linewidth=LW);
    #ax1.plot(sizes, rps, color=CO, ls='--',              linewidth=LW);
    #--------------------------------------------------------------------------------------------------------------------
    ax1.set_xscale('log');
    ax1.yaxis.grid(True, ls=':'); ax1.xaxis.grid(True, ls=':');
    ax1.yaxis.set_label_position('right');
    ax1.yaxis.set_major_locator(MaxNLocator(integer=True));
    ax1.yaxis.set_major_locator(MaxNLocator(prune='both'));
    #ax1.xaxis.set_major_locator(MaxNLocator(prune='upper'));
    ax1.tick_params(labelbottom=True, direction='in', bottom=True, top=True, left=True, right=True);
    if n_cols==column+1:
        ax1.set_ylabel('PRECISION'); ax1.yaxis.set_label_position('right');
    ax1.set_xlabel(' '.join(specification.split('_')[2:])+'\n'+generalization); ax1.xaxis.set_label_position('top');
    plt.setp(ax1.get_xticklabels()[-2:], visible=False)
    #--------------------------------------------------------------------------------------------------------------------
    ax2 = axes[1,column];
    #--------------------------------------------------------------------------------------------------------------------
    ax2.plot(sizes, lrs, color=CO, ls='-',  label=label, linewidth=LW);
    #ax2.plot(sizes, rrs, color=CO, ls='--',              linewidth=LW);
    #--------------------------------------------------------------------------------------------------------------------
    #ax2.set_xscale('log');
    ax2.yaxis.grid(True, ls=':'); ax2.xaxis.grid(True, ls=':');
    ax2.yaxis.set_major_locator(MaxNLocator(integer=True));
    ax2.yaxis.set_major_locator(MaxNLocator(prune='both'));
    #ax2.xaxis.set_major_locator(MaxNLocator(prune='upper'));
    ax2.yaxis.set_label_position('right');
    ax2.tick_params(labelbottom=False, direction='in', bottom=True, top=True, left=True, right=True);
    #ax2.legend(loc='center left', bbox_to_anchor=(1.1,0.5), fontsize=7, framealpha=1, edgecolor='k');
    if n_cols==column+1:
        ax2.set_ylabel('RECALL'); ax2.yaxis.set_label_position('right');
    if n_cols==column+1:
        ax2.set_xlabel('max gold size'); ax2.xaxis.set_label_position('bottom');#ax2.xaxis.set_label_coords(1.02,-0.05);


con           = sqlite3.connect(_infile);
cur           = con.cursor();
combinats     = cur.execute('SELECT DISTINCT representation,specification,generalization,max_label_size FROM results WHERE max_gold_size<10000 AND eval_mode="pair" AND specification IN ("'+'", "'.join(['restrictions_publications_'+rep+['','_mentions'][_mentions] for rep in _reps])+'") AND generalization IN ("'+'", "'.join(_gens)+'") ORDER BY specification,generalization,max_label_size').fetchall();
column2method = cur.execute('SELECT DISTINCT specification,generalization FROM results WHERE max_gold_size<10000 AND eval_mode="pair" AND specification IN ("'+'", "'.join(['restrictions_publications_'+rep+['','_mentions'][_mentions] for rep in _reps])+'") AND generalization IN ("'+'", "'.join(_gens)+'") ORDER BY specification,generalization').fetchall();
method2column = {tuple(column2method[i]):i for i in range(len(column2method))};

plt.rcParams.update({'axes.xmargin': 0});
plt.rcParams.update({'font.size':    7});

n_cols    = len(column2method);
fig, axes = plt.subplots(2, n_cols, sharex='all', sharey='row'); 
axes      = axes.reshape((2,n_cols));

#TODO: Solve the problem with the overlapping last and first xlabels

for representation,specification,generalization,max_label_size in combinats:
    #print(representation,specification,generalization,max_label_size);
    column  = method2column[(specification,generalization,)]; print(specification,generalization,column)
    plot(cur,representation,specification,generalization,1,max_label_size,column,n_cols);
    #plot_rest(cur,representation,specification,generalization,1,column,n_cols);

plt.subplots_adjust(hspace=0.15); plt.subplots_adjust(wspace=0.05); fig.set_size_inches((4*n_cols)/2.54,8/2.54,forward=True);

if not _outfile:
    plt.show();
else:
    plt.savefig(_outfile,dpi=600,bbox_inches="tight");

con.close();
