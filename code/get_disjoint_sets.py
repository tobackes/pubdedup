
def unique_indeces(seq):
    seen = set()
    seen_add = seen.add
    return [i for i in xrange(len(seq)) if not (seq[i] in seen or seen_add(seq[i]))];

repIDs  = ['a','ab','abc','b','c','ac','ad','bc','ce','fg','ghf','g','hi','hij'];
repIDs_ = ['zx','x','cax','z','y','kl','lz','ky','xy','abc','az','pl','pabcx','xyz'];
reps    = [set(repID) for repID in repIDs ];
reps_   = [set(repID) for repID in repIDs_];

#TODO: How to parallize?

def connected_components(repIDs,reps,parent=None,number=None,size2indeces=None,el2sets=None):
    #- PREPARATION ONLY -------------------------------------
    #indeces      = unique_indeces(repIDs);
    #repIDs       = [repIDs[i] for i in indeces];
    #reps         = [reps[i]   for i in indeces];
    if size2indeces == None:
        size2indeces = dict();
        el2sets      = dict();
        for i in xrange(len(repIDs)):
            size = len(reps[i]);
            if size in size2indeces:
                size2indeces[size].add(i);
            else:
                size2indeces[size] = set([i]);
                el2sets[size]      = dict();
            for el in reps[i]:
                if el in el2sets[size]:
                    el2sets[size][el].add(i);
                else:
                    el2sets[size][el] = set([i]);
    #--------------------------------------------------------
    #- THE ACTUAL SEARCH ------------------------------------
    parent     = range(len(repIDs)) if parent == None else parent;
    number     = [1 for i in xrange(len(repIDs))] if number == None else number;
    unassigned = {size: set(size2indeces[size]) for size in size2indeces};
    for size in sorted(unassigned.keys()):
        while not len(unassigned[size]) == 0:
            i              = unassigned[size].pop();
            #------------------------
            specifications = set([]);
            for size_ in [size__ for size__ in el2sets if size__ > size]:
                els             = [el for el in reps[i] if el in el2sets[size_]];
                specifications |= set([]) if len(els) != len(reps[i]) else set.intersection(*[el2sets[size_][el] for el in els]);
            print repIDs[i]; print [repIDs[spec] for spec in specifications];
            #------------------------
            for j in specifications:
                #------------------------ set_i = FIND(i)
                set_i,z = None,i;
                while True:
                    set_i = parent[z];
                    if set_i == z: break;
                    z = set_i;
                #------------------------ set_j = FIND(j)
                set_j,z = None,j;
                while True:
                    set_j = parent[z];
                    if set_j == z: break;
                    z = set_j;
                #------------------------ UNION(set_i,set_j)
                if set_i != set_j:
                    wini        = number[set_i] >= number[set_j];
                    fro         = [set_i,set_j][wini];
                    to          = [set_j,set_i][wini];
                    parent[fro] = to;
                    number[to] += number[fro];
                unassigned[len(reps[j])] -= set([j]);
    #------------------------------------ COMPRESS(parent)
    print parent;
    print number;
    for i in xrange(len(parent)):
        par,z,num = parent[i],i,number[i];
        while par != z:
            print z, par, number[par];
            number[par] -= num;
            z            = par;
            par = parent[z];
            if par == z:
                number[par] += num;
        parent[i] = par;
    print parent;
    print number;
    #------------------------------------ RESULT
    result = dict();
    for i in xrange(len(parent)):
        if parent[i] in result:
            result[parent[i]].add(repIDs[i]);
        else:
            result[parent[i]] =set([repIDs[i]]);
    print '#########################\n',result,'\n';
    #------------------------------------
    return repIDs, reps, parent, number, size2indeces, el2sets;

def merge(d, u):
    for k, v in u.iteritems():
        if (not k in d) or d[k] == None:
            d[k] = v;
        elif isinstance(v,dict) and v != {}:
            d[k] = merge(d.get(k,{}),v);
        elif isinstance(v,set):
            d[k] = d[k] | v;
        elif isinstance(v,list):
            d[k] = d[k] + v;
        elif isinstance(v,int) or isinstance(v,float):
            d[k] = d[k] + v;
        elif v != dict():
            d[k] = v;
    return d;

def combine(repIDss,repss,parents,numbers,size2indecess,el2setss):
    offset       = len(parents[0]);
    parent       = parents[0] + [index+offset for index in parents[1]];
    number       = numbers[0] + numbers[1];
    repIDs       = repIDss[0] + repIDss[1];
    reps         = repss[0]   + repss[1];
    sizes        = sorted((list(set(size2indecess[0].keys())|set(size2indecess[1].keys()))));
    size2indeces = {size:size2indecess[0][size] if size in size2indecess[0] else set()  for size in sizes};
    el2sets      = {size:el2setss[0][size]      if size in el2setss[0]      else dict() for size in sizes};
    for size in size2indecess[1]:
        size2indeces[size] |= set([offset+index for index in size2indecess[1][size]]);
        el2sets[size]       = merge(el2sets[size],{el:set([index+offset for index in el2setss[1][size][el]]) for el in el2setss[1][size]});
    return repIDs, reps, parent, number, size2indeces, el2sets;

results   = connected_components(repIDs, reps);
results_  = connected_components(repIDs_,reps_);
print {results[0][i]:results[3][i] for i in xrange(len(results[0]))};
results__ = combine(*zip(results,results_));
print connected_components(*results__);

