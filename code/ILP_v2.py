#-IMPORTS-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
import sys
import math
import mip
from itertools import product

#-MODEL PARAMETERS--------------------------------------------------------------------------------------------------------------------------------------------------------------------------
w_initial   = int(sys.argv[1]);#10#8;
w_firstname = int(sys.argv[2]);#2#2;
w_term      = int(sys.argv[3]);#10#8;
w_year      = int(sys.argv[4]);#1#1;

g_initial   = int(sys.argv[5]);#3; #4 out of 4
g_firstname = int(sys.argv[6]);#0; #4 out of 4
g_term      = int(sys.argv[7]);#3; #2 out of 6
g_year      = int(sys.argv[8]);#1; #1 out of 2

n_initial   = 4;
n_firstname = 4;
n_term      = 6;
n_title     = 6;
n_year      = 2;

m_initial   = 3;
m_firstname = 0;
m_term      = 2;
m_year      = 1;

m = n_initial*w_initial + n_firstname*w_firstname + n_term*w_term + n_year*w_year;
n = g_initial*w_initial + g_firstname*w_firstname + g_term*w_term + g_year*w_year;

slack    = math.floor(m*0.1*int(sys.argv[9]));
max_gens = int(sys.argv[10]);
outfile  = sys.argv[11] if len(sys.argv)>11 else None;
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#-INPUT-OUTPUT------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#v_initial   = int(sys.argv[1]);
#v_firstname = int(sys.argv[2]);
#v_term      = int(sys.argv[3]);
#v_year      = int(sys.argv[4]);
inputs = [seq for seq in product( *[ range(x+1) for x in [n_initial,n_firstname,n_term,n_year]]) if seq[0]>=seq[1] and seq[0]>1 and seq[2]>0 and seq[3]==2 and sum(seq)>6];
OUT    = open(outfile,'w') if outfile else None;
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#-SCRIPT------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
for v_initial,v_firstname,v_term,v_year in inputs:

    vs = [v_initial,v_firstname,v_term,v_year];
    c  = w_initial*v_initial + w_firstname*v_firstname + w_term*v_term + w_year*v_year;

    all_bound = math.floor((n/m)*c);
    all_bound = all_bound+1 if all_bound%2==0 else all_bound; # Make sure it is uneven number so that one year is always removed

    l_initial, l_firstname, l_term, l_year = math.ceil((m_initial/n_initial)*v_initial), math.ceil((m_firstname/n_firstname)*v_firstname), math.ceil((m_term/n_term)*v_term), math.ceil((m_year/n_year)*v_year);

    lbs = [l_initial,l_firstname,l_term,l_year];
    ubs = sorted([ub for ub in product(*[list(range(lbs[i],vs[i]+1)) for i in range(len(vs))]) if ub[0]*w_initial+ub[1]*w_firstname+ub[2]*w_term+ub[3]*w_year>=all_bound],reverse=True); #all possible upper bounds that would satisfy the all_bound

    optimal_value = c;
    solutions     = [];

    while len(ubs) > 0:

        u_initial, u_firstname, u_term, u_year = ubs.pop();

        model = mip.Model(); model.verbose=False;

        initials   = model.add_var(name='initials',   var_type=mip.INTEGER, lb=l_initial  );
        firstnames = model.add_var(name='firstnames', var_type=mip.INTEGER, lb=l_firstname);
        terms      = model.add_var(name='terms',      var_type=mip.INTEGER, lb=l_term     );
        years      = model.add_var(name='years',      var_type=mip.INTEGER, lb=l_year     );

        model.objective = mip.xsum([w_initial*initials, w_firstname*firstnames, w_term*terms, w_year*years]);

        model += mip.xsum([w_initial*initials, w_firstname*firstnames, w_term*terms, w_year*years])                         >= all_bound;   # general objective
        model += initials                                                                                                   <= u_initial;   # upper bound initials
        model += firstnames                                                                                                 <= u_firstname; # upper bound firstnames
        model += terms                                                                                                      <= u_term;      # upper bound terms
        model += years                                                                                                      <= u_year;      # upper bound years
        model += firstnames                                                                                                 <= initials;    # no firstnames without initials
        #model += ((v_initial-initials)*initials+1) * ((v_firstname-firstnames)*firstnames+1) * ((v_term-terms)*terms+1) * 2 <= max_gens;
        #model += (4+2*(1+v_initial-initials.x)%2) * (4+2*(1+v_firstname-firstnames.x)%2) * (4+2*(1+v_term-terms.x)%2) * 2 <= max_gens;

        model.optimize();

        if initials.x != None and model.objective_value <= optimal_value + slack:
            optimal_value = min([optimal_value,model.objective_value]);
            solution      = (int(initials.x),int(firstnames.x),int(terms.x),int(years.x),);
            footprint     = math.prod([math.comb(vs[i],vs[i]-solution[i]) for i in range(len(solution))]);
            #if footprint <= max_gens:
            solutions.append((model.objective_value,footprint,solution,));

    print(int(optimal_value),'out of',all_bound);

    solutions  = sorted(list(set(solutions)),reverse=True);
    #footprints = [math.prod([math.comb(vs[i],vs[i]-solution[i]) for i in range(len(solution))]) for solution,objective in solutions];
    solutions_ = [];
    i          = 0;
    while i < len(solutions):
        if solutions[i][1] < max_gens or ( len(solutions_)==0 and i+1==len(solutions )):
            solutions_.append(solutions[i]);
        i += 1;
    solutions = solutions_;

    if OUT and len(solutions)>0:
        for v_title in range(n_title+1):
            OUT.write(str(v_initial)+' initial '+str(v_firstname)+' first '+str(v_term)+' term '+str(v_year)+' year '+str(v_title)+' title --> '+' | '.join( (str(solution[-1][0])+' initial '+str(solution[-1][1])+' first '+str(solution[-1][2])+' term '+str(solution[-1][3])+' year 0 title' for solution in solutions))+'\n' );

    print(str(v_initial)+' initial '+str(v_firstname)+' first '+str(v_term)+' term '+str(v_year)+' year --> '+' | '.join( (str(rhs[0])+' initial '+str(rhs[1])+' first '+str(rhs[2])+' term '+str(rhs[3])+' year'+' ['+str(int(objective))+'/'+str(int(footprint))+']' for objective,footprint,rhs in solutions) )+'   ('+str(sum((footprint for _,footprint,_ in solutions)))+')' );

if OUT:
    OUT.close();
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
