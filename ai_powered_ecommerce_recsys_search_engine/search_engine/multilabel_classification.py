import os

import pandas as pd
from joblib import dump, load

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn.pipeline import Pipeline
from skmultilearn.problem_transform import BinaryRelevance, ClassifierChain, LabelPowerset

from data.data_cleanup.text_cleanup import pre_processing


def train_multilabel_classification(file_path):
    df = pd.read_csv(file_path)
    items = []
    for x in df["Product Name"]:
        text = pre_processing(x)
        items.append(text)

    labels = df.iloc[:, 1:-1]
    vectorizer = TfidfVectorizer(strip_accents='unicode', analyzer='word', ngram_range=(1, 2), norm='l2')
    x_train, x_test, y_train, y_test = train_test_split(items, labels)

    pipeline_binary = train_binary_relevance(x_train, y_train, x_test, y_test, vectorizer)
    pipeline_chain = train_classifier_chain(x_train, y_train, x_test, y_test, vectorizer)
    pipeline_powerset = train_label_powerset(x_train, y_train, x_test, y_test, vectorizer)
    return pipeline_binary, pipeline_chain, pipeline_powerset


def train_binary_relevance(x_train, y_train, x_test, y_test, vectorizer):
    pipeline_binary_relevance = Pipeline([
        ('vectorizer', vectorizer),
        ('clf', BinaryRelevance(GaussianNB()))
    ])
    pipeline_binary_relevance.fit(x_train, y_train)
    predictions = pipeline_binary_relevance.predict(x_test)
    print(accuracy_score(y_test, predictions))
    return pipeline_binary_relevance


def train_classifier_chain(x_train, y_train, x_test, y_test, vectorizer):
    pipeline_classifier_chain = Pipeline([
        ('vectorizer', vectorizer),
        ('clf', ClassifierChain(GaussianNB()))
    ])
    pipeline_classifier_chain.fit(x_train, y_train)
    predictions = pipeline_classifier_chain.predict(x_test)
    print(accuracy_score(y_test, predictions))
    return pipeline_classifier_chain


def train_label_powerset(x_train, y_train, x_test, y_test, vectorizer):
    pipeline_label_powerset = Pipeline([
        ('vectorizer', vectorizer),
        ('clf', LabelPowerset(GaussianNB()))
    ])
    pipeline_label_powerset.fit(x_train, y_train)
    predictions = pipeline_label_powerset.predict(x_test)
    print(accuracy_score(y_test, predictions))
    return pipeline_label_powerset


def save_pipeline_locally(pipeline, name):
    dump(pipeline, os.getcwd() + "/data/model_location/" + name)


def classify_query(query):
    query = pre_processing(query)
    return classify(query, "binary_relevance.joblib"), classify(query, "classifier_chain.joblib"), classify(query, "label_powerset.joblib")


def classify(query, file):
    clf = load(os.getcwd() + "/data/model_location/" + file)
    return clf.predict([query])