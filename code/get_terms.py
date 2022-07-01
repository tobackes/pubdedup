import re
from nltk.corpus import wordnet as WN
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from nltk.corpus import wordnet as WN
from unidecode import unidecode as UD
import asciidammit as dammit
from symspellpy import SymSpell, Verbosity
import pkg_resources

_ratio = 0.2;

_sym_spell       = SymSpell(max_dictionary_edit_distance=4, prefix_length=7);
_dictionary_path = pkg_resources.resource_filename("symspellpy", "frequency_dictionary_en_82_765.txt");
_stopwords       = set().union(*[set(stopwords.words(lang)) for lang in ['english']]);

_sym_spell.load_dictionary(_dictionary_path, term_index=0, count_index=1);

_stopwords = set().union(*[set(stopwords.words(lang)) for lang in ['english','german','french','italian','spanish','russian','portuguese','dutch','swedish','danish','finnish']]);
_tokenizer = RegexpTokenizer(r'\w+')
_surpres   = set(['de','del','di','de la','von','van','della']);

WORD      = re.compile(r'(\b[^\s]+\b)'); #TODO: Make stricter
STRIP     = re.compile(r'(^(\s|,)+)|((\s|,)+$)');
PUNCT     = re.compile(r'[!"#$%&\'()*+\/:;<=>?@[\\\]^_`{|}~1-9]'); #Meaningless punctuation for Author name lists, excludes , . -
SUBTITDIV = re.compile(r'\. |: | -+ |\? ');
STOPWORDS = re.compile(r'&|\.|\,|'+r'|'.join(['\\b'+stopword+'\\b' for stopword in _stopwords]));

def concat(object1, object2):
    if isinstance(object1, str):
        object1 = [object1]
    if isinstance(object2, str):
        object2 = [object2]
    return object1 + object2

def capitalize(word):
    return word[0].upper() + word[1:]

def generalize_0(term):
    if is_word(term):
        for synset in WN.synsets(term):
            yield synset;

def is_word(string):
    return len(string) > 2 and (string in _stopwords or len(WN.synsets(string)) > 0 or len(_sym_spell.lookup(string, Verbosity.CLOSEST,max_edit_distance=0, include_unknown=False))>0);

def splitter(string, language='en_us'):
    for index, char in enumerate(string):
        left_compound         = string[0:-index];
        right_compound_1      = string[-index:];
        right_compound_2      = string[-index+1:];
        right_compound1_upper = right_compound_1[0].isupper() if right_compound_1 else None;
        right_compound2_upper = right_compound_2[0].isupper() if right_compound_2 else None;
        left_compound         = capitalize(left_compound) if index > 0 and len(left_compound) > 1 and not is_word(left_compound) else left_compound;
        left_compound_valid   = is_word(left_compound);
        #print(left_compound,right_compound_1,right_compound_2,right_compound1_upper,right_compound2_upper,left_compound,left_compound_valid);
        if left_compound_valid and ((not splitter(right_compound_1,language) == '' and not right_compound1_upper) or right_compound_1 == ''):
            return [compound for compound in concat(left_compound, splitter(right_compound_1, language)) if not compound == ''];
        if left_compound_valid and string[-index:-index+1] == 's' and ((not splitter(right_compound_2, language) == '' and not right_compound2_upper) or right_compound_2 == ''):
            return [compound for compound in concat(left_compound, splitter(right_compound_2, language)) if not compound == ''];
    return [string] if not string == '' and is_word(string) else [capitalize(string)] if not string == '' and is_word(capitalize(string)) else '';

def split(string, language='en_us'):
    if string in _stopwords or len(WN.synsets(string)) > 0:
        return [string.lower()];
    result = splitter(string,language);
    return [string.lower()] if result=='' else [word.lower() for word in result];

def correct(string):
    suggestions = _sym_spell.lookup(string, Verbosity.CLOSEST,max_edit_distance=int(len(string)*_ratio), include_unknown=True);
    return suggestions[0].term if len(suggestions) > 0 else string;

def get_terms(title):
    if title == None:
        return [];
    terms    = [];
    title    = title.lower();
    titles   = SUBTITDIV.split(title);
    sections = [division for title in titles for division in STOPWORDS.split(title)];
    for section in sections:
        words     = [correct(dammit.asciiDammit(word)) for word in _tokenizer.tokenize(section) if not word in _stopwords];
        words     = [word_ for word in words for word_ in split(word)];
        wor,ds    = ([word for word in words if is_word(word)], [word for word in words if not is_word(word)]);
        words     = wor+ds;
        for word in words:
            synsets_0 = list(generalize_0(word));
            lemmas_0  = sorted([(lemma.count(),lemma.name(),) for synset in synsets_0 for lemma in synset.lemmas()], reverse=True) if len(synsets_0) > 0 else [(0,word,)];
            terms.append(lemmas_0[0][1]);
    check  = set([]);
    terms_ = [];
    for term in terms:
        if not term in check:
            terms_.append(term);
            check.add(term);
    return [(term,None,) for term in terms_];


while True:
    string = input("Please enter a title and I shall give you its terms: ");
    print([term for term,nothing in get_terms(string)]);
