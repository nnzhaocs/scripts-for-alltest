from __future__ import print_function
import numpy as np
import pandas as pd
import nltk
import re
import os
import codecs
from sklearn import feature_extraction
import mpld3
from nltk.stem.snowball import SnowballStemmer
#from sklearn.feature_extraction.text import TfidVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import KMeans
from sklearn.externals import joblib
import time
import logging

"""read from image file:
    format: image_name \t description
    """

image_name_file_abs = "all_english_unique_image_description.txt"
image_cluster_file_abs = "image_clusters.pkl"
#output_file_abs = "description_only.txt"
#output_fd = open(output_file_abs, "w")

num_clusters = 15

stopwords = nltk.corpus.stopwords.words('english')
stemmer = SnowballStemmer("english")

#image_description_arr = image_arr[:,1]
#image_names=image_arr[:,0]

def load_description(image_name_file_abs, image_arr):
    logging.debug("read from file:%s", image_name_file_abs)
    with open(image_name_file_abs) as fd:
        for line in fd:
            #print line.split("\t")
            image_description = line.split("\t")[0]
            if image_description == " ":
                continue
            image_name = line.split("\t")[1].replace("\n","")
            image_arr = np.append(image_arr, ([image_name, image_description],), axis=0)
            #output_fd.write(image_description+"\t"+image_name+"\n")
    logging.debug("finished reading file:%s", image_name_file_abs)
#print image_arr

def tokenize_and_stem(text):
    logging.debug("first tokenize by sentence, then by word to ensure that punctuation is caught as it's own token")
    tokens = [word for sent in nltk.sent_tokenize(text) for word in nltk.word_tokenize(sent)]
    filtered_takens = []
    logging.debug("filter out any tokens not containing letters (e.g., numeric tokens, raw punctuation)")
    for token in tokens:
        if re.search('[a-zA-Z]', token):
            filtered_takens.append(token)

    stems = [stemmer.stem(t) for t in filtered_takens]
    logging.debug("finished!")
    return stems


def tokenize_only(text):
    logging.debug("first tokenize by sentence, then by word to ensure that punctuation is caught as it's own token")
    tokens = [word.lower() for sent in nltk.sent_tokenize(text) for word in nltk.word_tokenize(sent)]
    filtered_tokens = []
    logging.debug("filter out any tokens not containing letters (e.g., numeric tokens, raw punctuation)")
    for token in tokens:
        if re.search('[a-zA-Z]', token):
            filtered_tokens.append(token)

    return filtered_tokens


def print_cluster(clusters, vocab_frame, image_description_arr, terms):
    logging.debug("loading from the pickle:%s", image_cluster_file_abs)
    km = joblib.load(image_cluster_file_abs)

    clusters = km.labels_tolist()

    image_names = image_arr[:, 0]
    images = {'name': image_names, 'description': image_description_arr, 'cluster': clusters}

    frame = pd.DataFrame(images, index=[clusters], columns=['name', 'cluster', 'description'])

    frame['cluster'].value_counts()

    print("Top terms per cluster:")

    print()

    order_centriods = km.cluster_centers.argsort()[:, ::-1]

    for i in range(num_clusters):
        print("Cluster %d words:" % i, end='')
        for ind in order_centriods[i, :6]:
            print(' %s' % vocab_frame.ix[terms[ind].split(' ')].values.tolist()[0][0].encode('utf-8', 'ignore'), end=',')
        print()
        print()

        print("Cluster %d names:" % i, end='')
        for name in frame.ix[i]['name'].values.tolist():
            print(' %s,' % name, end='')

        print()
        print()

    print()
    print()


def k_means_clustering(image_arr):
    """read from image file:
        format: image_name \t description
        """

    logging.debug("clustering starting: ......")

    image_description_arr = image_arr[:, 1]

    totalvocab_stemmed = []
    totalvocab_tokenized = []

    logging.debug("iterate over the list of descriptions to create two vocabularies: one stemmed and one only tokenized")

    for i in image_description_arr:
        allwords_stemmed = tokenize_and_stem(i)
        totalvocab_stemmed.extend(allwords_stemmed)

        allwords_tokenized = tokenize_only(i)
        totalvocab_tokenized.extend(allwords_tokenized)

    vocab_frame = pd.DataFrame({'words': totalvocab_tokenized}, index=totalvocab_stemmed)
    vocab_frame.drop_duplicates()

    tfidf_vectorizer = TfidfVectorizer(max_df=0.8, max_features=200000, min_df=0.2, stopwords='english',
                                      used_idf=True, tokenizer=tokenize_and_stem, ngram_range=(1,3))
    stat_time = time.time()
    tfidf_matrix = tfidf_vectorizer.fit_transform(image_description_arr)
    elapsed_time = time.time() - stat_time

    logging.debug("fit the vectorizer to description: elapsed_time=%.4f\n", elapsed_time)

    logging.debug("tfidf_matrix.shape:%d", tfidf_matrix.shape)

    terms = tfidf_vectorizer.get_feature_names()

    dist = 1 - cosine_similarity(tfidf_matrix)
    logging.debug("possible to evaluate the similarity of any two or more description: dist=%.4f\n", dist)

    km = KMeans(n_clusters = num_clusters)

    stat_time = time.time()
    km.fit(tfidf_matrix)
    elapsed_time = time.time() - stat_time

    logging.debug("time to converge a global optimum as k-means is susceptible to reaching local optima.: elapsed_time=%.4f\n", elapsed_time)

    clusters = km.labels_tolist()

    logging.debug("saving image_cluster_file_abs: %s", image_cluster_file_abs)
    joblib.dump(km, image_cluster_file_abs)

    print_cluster(clusters, vocab_frame, image_description_arr, terms)


def main():
    fmt = "%(funcName)s():%(lineno)i: %(message)s %(levelname)s"
    logging.basicConfig(level=logging.DEBUG, format=fmt)

    logging.debug("======================> clustering starting: <======================")

    image_arr = np.empty((0, 2))
    load_description(image_name_file_abs, image_arr)

    k_means_clustering(image_arr)


if __name__ == '__main__':
    print('start!')
    main()
    print('finished!')