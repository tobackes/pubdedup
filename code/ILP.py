import sys
import math
import mip

v_initial   = int(sys.argv[1]);
v_firstname = int(sys.argv[2]);
v_term      = int(sys.argv[3]);
v_year      = int(sys.argv[4]);

vs = [v_initial,v_firstname,v_term,v_year];

w_initial   = 8;
w_firstname = 2;
w_term      = 8;
w_year      = 1;

m = 74;
n = 57;

c = w_initial*v_initial + w_firstname*v_firstname + w_term*v_term + w_year*v_year;

all_bound = math.floor((n/m)*c);

l_initial   = math.ceil((3/m)*c);
l_firstname = math.ceil((0/m)*c);
l_term      = math.ceil((2/m)*c);
l_year      = math.ceil((1/m)*c);

lbs = [l_initial,l_firstname,l_term,l_year];

ubs = set([(v_initial,v_firstname,v_term,v_year,)]);

solutions = [];
bad_ubs   = set([]);
tried_ubs = set([]);
objective = 0;

while len(ubs) > 0:

    u_initial, u_firstname, u_term, u_year = ubs.pop();
    print(u_initial,u_firstname,u_term,u_year);
    model = mip.Model();

    initials   = model.add_var(name='initials',   var_type=mip.INTEGER, lb=l_initial  );
    firstnames = model.add_var(name='firstnames', var_type=mip.INTEGER, lb=l_firstname);
    terms      = model.add_var(name='terms',      var_type=mip.INTEGER, lb=l_term     );
    years      = model.add_var(name='years',      var_type=mip.INTEGER, lb=l_year     );

    model.objective = mip.xsum([w_initial*initials, w_firstname*firstnames, w_term*terms, w_year*years]);

    model += mip.xsum([w_initial*initials, w_firstname*firstnames, w_term*terms, w_year*years]) >= all_bound;
    model += initials   <= u_initial;
    model += firstnames <= u_firstname;
    model += terms      <= u_term;
    model += years      <= u_year;

    model.optimize();

    tried_ubs.add((u_initial,u_firstname,u_term,u_year,));

    if initials.x == None or model.objective_value < objective:
        bad_ubs.add( (u_initial,u_firstname,u_term,u_year,));
    else:
        objective = model.objective_value;
        solutions.append((int(initials.x),int(firstnames.x),int(terms.x),int(years.x),));
        print(solutions[-1]);
    minima = [min((solutions[i][j] for i in range(len(solutions)) )) for j in range(len(solutions[0]))];

    new_ubs = set([tuple(solutions[-1][:i]+[j]+solutions[-1][i+1:]) for i in range(len(minima)) for j in range(minima[i],lbs[i]-1,-1)]) - (bad_ubs | tried_ubs);
    print(new_ubs);

    for new_ub in new_ubs:
        ubs.add(new_ub);

solutions = set(solutions);

