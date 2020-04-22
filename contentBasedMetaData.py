import pandas as pd
from pyspark import SparkContext, join
import numpy as np
# The following features are present in the dataset
# weights for each feature


# TMDB_ID	Adult	Budget	Genre_ID	Genre_Name	            IMDB_ID	    Movie_Name	Language	Synopsis	Popularity	Production_Company_IDs	Production_Company_Names	Production_Country_Names	Release_Date	Running_Time	Vote_Count	Vote_Average	Revenue	Tagline	Director	Cast	Screenplay	DOP	Editor	Music_Composer	Assistant_Director
# int	binary	        int 	list_of_ints	list_of_strings	tt0411267	Movie_Name	Language	Synopsis	Popularity	Production_Company_IDs	Production_Company_Names	Production_Country_Names	Release_Date	Running_Time	Vote_Count	Vote_Average	Revenue	Tagline	Director	Cast	Screenplay	DOP	Editor	Music_Composer	Assistant_Director
import constants as CONSTANTS
sc = SparkContext('local[*]', 'content-based')
sc.setLogLevel("ERROR")


def cosineSimilarityBetweenMovies():


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

    def commaSeperatedToList(input):
        # :-1 to trim the last comma
        list = input[:-1].split(',')
        return list


    # tmdb, imdb, name, genreid, adult, director, cast, asstnt direc, lang, prod_country
    def parse(x):
        # key is TMDB_ID for now
        y = x.split('\t')
        # return {'key' : (y[0], y[5], y[6], y[3], y[1],
        #         getListCombineNames(y[19]),
        #         getListCombineNames(y[20]),
        #         getListCombineNames(y[25]),
        #         y[7], y[12])}
        return {CONSTANTS.TMDB_ID: y[0],
                CONSTANTS.IMDB_ID: y[5].replace("tt", ""),
                CONSTANTS.MOVIE: y[6],
                CONSTANTS.GENRE_ID: commaSeperatedToList(y[3]),  # Genre Feature Set
                CONSTANTS.ADULT: y[1],  # Is Adult Boolean
                CONSTANTS.CAST: getListCombineNames(y[20]),  # Cast Feature Set
                }

    data = sc.textFile(CONSTANTS.MOVIE_METADATA_FILE)
    rdd_data = data.map(lambda x: parse(x))
    rdd_data = rdd_data.filter(lambda x: x.get(CONSTANTS.TMDB_ID) != CONSTANTS.METADATA_COLUMNS_MAP.get(CONSTANTS.TMDB_ID))
    movie_metadata = rdd_data.take(10)
    print(movie_metadata)

    casts = rdd_data.flatMap(lambda features: features.get(CONSTANTS.CAST)) \
        .distinct() \
        .collect()
    print("Total Number of Casts : {0}".format(len(casts)))
    print(casts)

    genreList = rdd_data.flatMap(lambda features: features.get(CONSTANTS.GENRE_ID)) \
        .distinct() \
        .filter(lambda x: x != '') \
        .collect()

    print("Total Number of Genre : {0}".format(len(genreList)))
    print(genreList)

    # Currently only on GENRE
    def generateItemProfile(features):
        movieGenreList = features.get(CONSTANTS.GENRE_ID)
        lorg = {CONSTANTS.IMDB_ID: features.get(CONSTANTS.IMDB_ID),
                CONSTANTS.TMDB_ID: features.get(CONSTANTS.TMDB_ID),
                CONSTANTS.MOVIE: features.get(CONSTANTS.MOVIE)
                }
        lorg["TOKENS"] = [features.get(CONSTANTS.ADULT)] + features.get(CONSTANTS.GENRE_ID)

        # for genre in genreList:
        #     if genre in movieGenreList:
        #         lorg["TOKENS"].append(1)
        #     else:
        #         lorg["TOKENS"].append(0)

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

    npItemFeatures = np.array(itemFeatures)

    from sklearn.metrics.pairwise import cosine_similarity
    from sklearn.feature_extraction.text import CountVectorizer

    # instantiating and generating the count matrix
    count = CountVectorizer()
    count_matrix = count.fit_transform(itemFeatures.map(lambda x: x[TOKENS]).collect())

    movieTitles = np.array(itemFeatures.map(lambda x: x[CONSTANTS.IMDB_ID]).collect())
    print(movieTitles)

    # generating the cosine similarity matrix
    cosine_sim = np.array(cosine_similarity(count_matrix, count_matrix))

    indexing = pd.Series(itemFeatures.map(lambda x: x[TITLE]).collect())
    print(indexing)
    # print(cosine_sim)
    return cosine_sim, indexing



def recommendUser(userId, movieId):
    pass

def findUserRecommendation(cosine_sim, indexing):
    def parse_MOVIEID_MOVIENAME_FILE(x):
        line = x.split("\t")
        movieId = line[0]
        movieName = line[1]
        return ( movieId, [movieName])

    def parse_MOVIEID_USERID_RATINGS_FILE(x):
        line = x.split("\t")
        movieId = line[0]
        userId = line[1]
        rating = line[2]
        return (movieId, [userId, rating])

    recommendUser(userId=4, movieId=9)

    result = []
    # Getting the id of the movie for which the user want recommendation
    ind = indices[movie].iloc[0]
    # Getting all the similar cosine score for that movie
    sim_scores = list(enumerate(cosine_sim[ind]))

    def modifyJoin(x):
        mid = x[0]
        l = x[1]

        mname = l[1][0]
        uid = l[0][0]
        rating = l[0][1]

        return (mname, [mid, uid, rating])

    modifiedJoin = joined.map(lambda x: modifyJoin(x))
    print(modifiedJoin.take(10))

    joined2 = rdd_data3.join(modifiedJoin)
    print(joined2.take(10))

    all = joined2.groupByKey().map(lambda x : (x[0], list(x[1])))
    print(all.take(10))

    # Say we find the user movieid rating matrix


if __name__ == '__main__':
    cosine_sim, indexing = cosineSimilarityBetweenMovies()
    findUserRecommendation(cosine_sim, indexing)



  # data1 = sc.textFile(CONSTANTS.MOVIEID_MOVIENAME_FILE)
    # rdd_data1 = data1.map(lambda x: parse_MOVIEID_MOVIENAME_FILE(x))
    # print(rdd_data1.take(10))
    #
    # data2 = sc.textFile(CONSTANTS.MOVIEID_USERID_RATINGS_FILE)
    # rdd_data2 = data2.map(lambda x: parse_MOVIEID_USERID_RATINGS_FILE(x))
    # print(rdd_data2.take(10))
    #
    # joined = rdd_data2.join(rdd_data1)
    # # movieId, movieName, userId
    # # final = joined.mapValues(lambda lineTuple: lineTuple)
    # print(joined.take(10))
    #
    # def parseTmdb(line):
    #     # moviename, tmdbid, imdbid
    #     movieName = line[6]
    #     tmdbid = line[0]
    #     imdbid = line[5]
    #     return ( movieName, [tmdbid, imdbid])
    #
    # data3 = sc.textFile(CONSTANTS.TMDB_MOVIE_METADATA_CLEANED)
    # rdd_data3 = data3.map(lambda x: parseTmdb(x))