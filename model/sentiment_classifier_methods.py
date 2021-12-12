import re
import pickle
from string import punctuation
import os
from nltk.tag import pos_tag
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer


#define stop words
stopwords = stopwords.words('english')

#grab classifier from pickle file
dirname = os.path.dirname(__file__)
classifier_filepath = os.path.join(dirname, 'model.pickle')
print("retrieving model from: ", classifier_filepath)
with open(classifier_filepath, 'rb') as f:
    classifier = pickle.load(f)

#define tokenizer
word_tokenizer = TweetTokenizer(
            preserve_case=True,
            reduce_len=False,
            strip_handles=False)

#define Lemmatizer
lemmatizer = WordNetLemmatizer()

def tokenize(tweet):
    return word_tokenizer.tokenize(tweet)

def lemmatize(tokens):
    return [
        lemmatizer.lemmatize(word, tag2type(tag)).lower()
        for word, tag in pos_tag(tokens) if not is_noise(word)
    ]

def is_noise(word):
    pattern = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+|(@[A-Za-z0-9_]+)'
    return word in punctuation \
           or word.lower() in stopwords \
           or re.search(pattern, word, re.IGNORECASE) != None

def tag2type(tag):
    """
    Take a tag and return a type
    Common tags are:
        - NNP: Noun, proper, singular
        - NN: Noun, common, singular or mass
        - IN: Preposition or conjunction, subordinating
        - VBG: Verb, gerund or present participle
        - VBN: Verb, past participle
        - JJ: adjective ‘big’
        - JJR: adjective, comparative ‘bigger’
        - JJS: adjective, superlative ‘biggest’
        - ...
    return 'n' for noun, 'v' for verb, and 'a' for any
    """
    if tag.startswith('NN'):
        return 'n'
    elif tag.startswith('VB'):
        return 'v'
    else:
        return 'a'

def classify(tweet):
    """

    :param tweet: raw text body of a tweet
    :return: sentiment, Positive or Negative
    """
    tweet = tweet.strip()
    tokens = lemmatize(tokenize(tweet))
    return classifier.classify(dict([token, True] for token in tokens))
