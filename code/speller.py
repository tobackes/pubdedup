import pkg_resources
from symspellpy import SymSpell, Verbosity

_dict_edit_dist = 4;
_ratio          = 0.2;

sym_spell       = SymSpell(max_dictionary_edit_distance=_dict_edit_dist, prefix_length=7);
dictionary_path = pkg_resources.resource_filename("symspellpy", "frequency_dictionary_en_82_765.txt");

sym_spell.load_dictionary(dictionary_path, term_index=0, count_index=1);


while True:
    string      = input("Enter word to check...");
    string      = string.rstrip().strip();
    #suggestions = sym_spell.lookup(string, Verbosity.CLOSEST,max_edit_distance=int(len(string)*_ratio), include_unknown=True);
    suggestions = [suggestion.term for suggestion in sym_spell.lookup(string, Verbosity.CLOSEST,max_edit_distance=min(_dict_edit_dist,int(len(string)*_ratio)), include_unknown=False)];
    print(suggestions[0] if len(suggestions) > 0 else string);
