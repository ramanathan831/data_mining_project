import pandas as pd
import numpy as np
import constants as CONSTANTS

from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer

def combineFullName(name):
    return name.lower().replace(" ", "")


def getListCombineNames(namesList):
    list = []
    for name in namesList.split(','):
        if len(name) != 0:
            list.append(combineFullName(name))
    return list


def isAdult(boolAdult):
    if boolAdult == 'false':
        return 0
    else:
        return 1


def commaSeparatedToList(input):
    # :-1 to trim the last comma
    list = getListCombineNames(input[1:-2])
    return list


def parse(x):
    y = x.split('\t')
    return {CONSTANTS.IMDB_ID: y[0],
            CONSTANTS.MOVIE: y[1],
            CONSTANTS.GENRES: commaSeparatedToList(y[2]),  # Genre Feature Set
            CONSTANTS.CAST: getListCombineNames(y[8]),  # Cast Feature Set
            CONSTANTS.LANGUAGE: getListCombineNames(y[12]),  # Cast Feature Set
            CONSTANTS.KEYWORDS: getListCombineNames(y[5]),  # Cast Feature Set
        }


def findSimilarity(sc):
    data = sc.textFile(CONSTANTS.MOVIE_METADATA_FILE)
    rdd_data = data.map(lambda x: parse(x))
    rdd_data = rdd_data.filter(
        lambda x: x.get(CONSTANTS.IMDB_ID) != CONSTANTS.IMDB_ID)
    movie_metadata = rdd_data.take(10)
    print("Movie Metadata", movie_metadata)

    casts = rdd_data.flatMap(lambda features: features.get(CONSTANTS.CAST)) \
        .distinct() \
        .collect()
    print("Total Number of Casts : {0}".format(len(casts)))
    print("Cast List", casts)

    genreList = rdd_data.flatMap(lambda features: features.get(CONSTANTS.GENRES)) \
        .distinct() \
        .filter(lambda x: x != '') \
        .collect()

    print("Total Number of Genre : {0}".format(len(genreList)))
    print("Genre List", genreList)

    # Currently only on GENRE, IS ADULT, CAST
    def generateItemProfile(features):
        lorg = {
            CONSTANTS.IMDB_ID: features.get(CONSTANTS.IMDB_ID),
            CONSTANTS.MOVIE: features.get(CONSTANTS.MOVIE),
            "TOKENS": features.get(CONSTANTS.GENRES)
                      + features.get(CONSTANTS.CAST)
                      + features.get(CONSTANTS.LANGUAGE)
                      + features.get(CONSTANTS.KEYWORDS)
        }


        return lorg

    TITLE = 'TITLE'
    TOKENS = 'TOKENS'

    itemProfile = rdd_data.map(lambda features: generateItemProfile(features))
    itemFeatures = itemProfile.map(
        lambda itemProf: {
            TITLE: itemProf[CONSTANTS.MOVIE],
            CONSTANTS.IMDB_ID: itemProf[CONSTANTS.IMDB_ID],
            TOKENS: (' '.join(itemProf[TOKENS])).strip(' ')
        }
    )

    # instantiating and generating the count matrix
    count = CountVectorizer()
    count_matrix = count.fit_transform(itemFeatures.map(lambda x: x[TOKENS]).collect())

    # movieIds_imdb = np.array(itemFeatures.map(lambda x: x[CONSTANTS.IMDB_ID]).collect())
    # print("Movie IMDB_ID: ", movieIds_imdb)

    # generating the cosine similarity matrix
    cosine_sim = np.array(cosine_similarity(count_matrix, count_matrix))

    imdbIds = itemFeatures.map(lambda x: x[CONSTANTS.IMDB_ID]).distinct().collect()
    indexes = []
    for i in range(0, len(imdbIds)):
        indexes.append(i)

    indexing = pd.Series(indexes, imdbIds)
    # print("MovieIds  |  Indexes ")
    # print(indexing)
    # print(cosine_sim)
    return cosine_sim, indexing


