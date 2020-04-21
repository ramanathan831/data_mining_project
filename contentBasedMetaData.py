from pyspark import SparkContext
import numpy as np
# The following features are present in the dataset
# weights for each feature


# TMDB_ID	Adult	Budget	Genre_ID	Genre_Name	            IMDB_ID	    Movie_Name	Language	Synopsis	Popularity	Production_Company_IDs	Production_Company_Names	Production_Country_Names	Release_Date	Running_Time	Vote_Count	Vote_Average	Revenue	Tagline	Director	Cast	Screenplay	DOP	Editor	Music_Composer	Assistant_Director
# int	binary	        int 	list_of_ints	list_of_strings	tt0411267	Movie_Name	Language	Synopsis	Popularity	Production_Company_IDs	Production_Company_Names	Production_Country_Names	Release_Date	Running_Time	Vote_Count	Vote_Average	Revenue	Tagline	Director	Cast	Screenplay	DOP	Editor	Music_Composer	Assistant_Director
sc = SparkContext('local[*]', 'content-based')
sc.setLogLevel("ERROR")

DATASET_DIR = "Datasets"
MOVIE_METADATA_FILE = DATASET_DIR + "/intersection_final_structured_general_metadata.tsv"
USER_MOVIE_RATINGS_FILE = DATASET_DIR + "/intersection_final_structured_general_metadata.tsv"



def cosineSimilarityBetweenMovies():
    METADATA_COLUMNS_MAP = {
        'TMDB_ID': 'TMDB_ID',
        'Movie_Name': 'Movie_Name',
        'Adult': 'Adult',
        'Genre_ID': 'Genre_ID',
        'IMDB_ID': 'IMDB_ID',
        'Language': 'Language',
        'Production_Country_Names': 'Production_Country_Names',
        'Director': 'Director',
        'Assistant_Director': 'Assistant_Director',
        'Cast': 'Cast'
    }

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
        list = input.split(',')
        return list

    IMDB_ID = 'imdbId'
    TMDB_ID = 'tmdbId'
    MOVIE = 'movie'
    GENRE_ID = 'genreIDList'
    CAST = 'castList'
    ADULT = 'isAdult'

    # tmdb, imdb, name, genreid, adult, director, cast, asstnt direc, lang, prod_country
    def parse(x):
        # key is TMDB_ID for now
        y = x.split('\t')
        # return {'key' : (y[0], y[5], y[6], y[3], y[1],
        #         getListCombineNames(y[19]),
        #         getListCombineNames(y[20]),
        #         getListCombineNames(y[25]),
        #         y[7], y[12])}
        return {TMDB_ID: y[0],
                IMDB_ID: y[5],
                MOVIE: y[6],
                GENRE_ID: commaSeperatedToList(y[3]),  # Genre Feature Set
                ADULT: y[1],  # Is Adult Boolean
                CAST: getListCombineNames(y[20]),  # Cast Feature Set
                }

    data = sc.textFile(MOVIE_METADATA_FILE)
    rdd_data = data.map(lambda x: parse(x))
    movie_metadata = rdd_data.take(10)
    print(movie_metadata)

    casts = rdd_data.flatMap(lambda features: features.get(CAST)) \
        .distinct() \
        .collect()
    print("Total Number of Casts : {0}".format(len(casts)))
    print(casts)

    genreList = rdd_data.flatMap(lambda features: features.get(GENRE_ID)) \
        .distinct() \
        .filter(lambda x: x != '') \
        .collect()

    print("Total Number of Genre : {0}".format(len(genreList)))
    print(genreList)

    # Currently only on GENRE
    def generateItemProfile(features):
        movieGenreList = features.get(GENRE_ID)
        lorg = {IMDB_ID: features.get(IMDB_ID), TMDB_ID: features.get(TMDB_ID), MOVIE: features.get(MOVIE)}
        lorg["TOKENS"] = [features.get(ADULT)] + features.get(GENRE_ID)

        # for genre in genreList:
        #     if genre in movieGenreList:
        #         lorg["TOKENS"].append(1)
        #     else:
        #         lorg["TOKENS"].append(0)

        return lorg

    itemProfile = rdd_data.map(lambda features: generateItemProfile(features))
    itemFeatures = itemProfile.map(
        lambda itemProf: {'TITLE': itemProf[MOVIE], 'TOKENS': (' '.join(itemProf["TOKENS"])).strip(' ')})

    npItemFeatures = np.array(itemFeatures)

    from sklearn.metrics.pairwise import cosine_similarity
    from sklearn.feature_extraction.text import CountVectorizer

    # instantiating and generating the count matrix
    count = CountVectorizer()
    count_matrix = count.fit_transform(itemFeatures.map(lambda x: x['TOKENS']).collect())

    np.array(itemFeatures.map(lambda x: x['TITLE']).collect())

    # generating the cosine similarity matrix
    cosine_sim = np.array(cosine_similarity(count_matrix, count_matrix))

    print(cosine_sim)
    return cosine_sim


def findUserRecommendation():
    def parse(x):
        return x.split(",")

    data = sc.textFile(USER_MOVIE_RATINGS_FILE)
    rdd_data = data.map(lambda x: parse(x))

    def createUserMap(x):
        return (x[0])

    # userMap = rdd_data.map(lambda x: )

if __name__ == '__main__':
    cosine_sim = cosineSimilarityBetweenMovies()
    findUserRecommendation()
