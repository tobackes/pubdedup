import sys

_infile  = sys.argv[1];
_outfile = sys.argv[2];

_type = True;


def generalizes(a,b):
    for i in range(len(a)):
        if a[i] > b[i]:
            return False;
    return True;


#_conditionals = [];
_nodes        = set([]);
_generals     = dict();
IN = open(_infile);
for line in IN.readlines():
    lhs, rhs      = line.rstrip().split(' --> ');
    lhs, rhs      = lhs.split(), [part.split() for part in rhs.split(' | ')];
    lhs           = [(int(lhs[i]),lhs[i+1],) for i in range(0,len(lhs),2)];
    rhs           = [[(int(part[i]),part[i+1],) for i in range(0,len(part),2)] for part in rhs];
    #_conditionals = [typ for freq,typ in lhs];
    _generals[tuple([[freq,typ+'|'][_type] for freq,typ in lhs if not _type or freq>0])] = [tuple([[freq,typ+'|'][_type] for freq,typ in part if not _type or freq>0]) for part in rhs];
    _nodes.add(tuple([[freq,typ+'|'][_type] for freq,typ in lhs if not _type or freq>0]));
    for part in rhs:
        _nodes.add(tuple([[freq,typ+'|'][_type] for freq,typ in part if not _type or freq>0]));
IN.close();

edges_rule = dict();
edges_gens = dict();

for lhs in _generals:
    for rhs in _generals[lhs]:
        edge = '"'+''.join([str(num) for num in rhs])+'" -> "'+''.join([str(num) for num in lhs])+'"';
        edges_rule[edge] = '[color=blue]';

for lhs in _nodes:
    for rhs in _nodes:
        if lhs != rhs and _type or generalizes (lhs,rhs): #TODO: Do we need this?
            edge = '"'+''.join([str(num) for num in lhs])+'" -> "'+''.join([str(num) for num in rhs])+'"';
            if not edge in edges_rule:
                if _type:
                    continue;
                edges_gens[edge] = '[style=dotted penwidth=0.4]';

OUT = open(_outfile.split('.')[0]+'_rule.txt','w');
for edge in edges_rule:
    OUT.write(edge+' '+edges_rule[edge]+'\n');
OUT.write('\n}');
OUT.close();

OUT = open(_outfile.split('.')[0]+'_gens.dot','w');
OUT.write('digraph G {\n\nsplines=line\nranksep=0.3\nnode [shape=box style="filled,rounded" fillcolor=black fontcolor=white margin=0.01 width=0 height=0]\nedge [penwidth=2 arrowhead=none];\n\n');
for edge in edges_gens:
    OUT.write(edge+' '+edges_gens[edge]+'\n');
OUT.write('\n}');
OUT.close();
