import unicodedata

import nltk
import pandas as pd
from bs4 import BeautifulSoup
from nltk.corpus import stopwords
from nltk.tokenize import ToktokTokenizer
from sklearn.feature_extraction.text import TfidfVectorizer

nltk.download("stopwords")
nltk.download("wordnet")


stopWordList = stopwords.words('english')
stopWordList.remove('no')
stopWordList.remove('not')

lemma = nltk.WordNetLemmatizer()
token = ToktokTokenizer()

ecommerce_stop_word_list = ["pack", "oz", "fl", "ct", "ea", "x", "g", "od"]


def remove_tags(data):
    soup = BeautifulSoup(data, 'html.parser')
    text = soup.get_text()
    return text


def remove_ascending_char(data):
    data = unicodedata.normalize('NFKD', data).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return data


def remove_char_digit(text):
    str = '`1234567890-=~@#$%^&*()_+[!{;":\'><.,/?"}]'
    for w in text:
        if w in str:
            text = text.replace(w, '')
    return text


def lemmatize_words(text):
    words = token.tokenize(text)
    listLemma = []
    for w in words:
        x = lemma.lemmatize(w, 'v')
        listLemma.append(x)
    return text


def stop_words_remove(text):
    wordList = [x.lower().strip() for x in token.tokenize(text)]

    removedList = [x for x in wordList if not x in stopWordList]
    text = ' '.join(removedList)
    # print(text)
    return text


def ecommerce_stop_words_remove(text):
    words = text.split(' ')
    for word in words:
        if word in ecommerce_stop_word_list:
            text = text.replace(word, '')
    text = text.strip()
    return text


def pre_processing(text):
    text = remove_char_digit(text)
    text = remove_ascending_char(text)
    text = lemmatize_words(text)
    text = stop_words_remove(text)
    text = ecommerce_stop_words_remove(text)
    return text

