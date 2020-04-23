from pyspark import SparkContext
import constants as CONSTANTS

from ContentBasedFiltering.moviemovieCosineSimilarity import *

# The following features are present in the dataset
# weights for each feature
sc = SparkContext('local[*]', 'content-based')
sc.setLogLevel("ERROR")

MOVIEID = 'movieId'
RATINGS = 'ratings'


def getUserRatingsRdd():

    def parse(x):
        return int(x[0]), {MOVIEID: x[1], RATINGS: x[2]}

    data = sc.textFile(CONSTANTS.MOVIEID_USERID_RATINGS_FILE)
    rdd_data = data\
        .map(lambda x: x.split("\t"))\
        .filter(lambda x: x[0] != CONSTANTS.HEADER.get(CONSTANTS.USER_ID))\
        .map(lambda x: parse(x))\
        .groupByKey()\
        .map(lambda x: (x[0], list(x[1])))

    return rdd_data


def recommendMovieToUser(userId, movieId, cosine_sim, indexing):
    rdd = getUserRatingsRdd()
    print("User Movie Ratings : ", rdd.take(10))

    # Getting the id of the movie for which the user want recommendation
    movieIndex = indexing[movieId]
    print("Index in Similarity Matrix", movieIndex)

    # Getting all the similar cosine score for that movie
    sim_scores = list(cosine_sim[movieIndex])
    print("Similarity scores for all other movies with this movie : ", sim_scores)
    print('The Movie You Should Watched Next Are --')
    print('ID ,   Name ,  Average Ratings , Predicted Rating ')
    # Varible to print only top 10 movies
    count = 0

    def parseUserRatings(x):
        if userId == x[0]:
            return True
        else:
            return False

    # Get only the userId row that we need
    userMovieRatings = rdd.filter(lambda x: parseUserRatings(x)).collect()[0][1]
    print("userMovieRatings", userMovieRatings)

    sum = 0
    weights = 0
    for userRatedMovie in userMovieRatings:
        otherMovieId = userRatedMovie.get(MOVIEID)
        otherMovieRating = float(userRatedMovie.get(RATINGS))

        if len(otherMovieId) == 6:
            otherMovieId = '0' + str(otherMovieId)

        otherMovieIndex = indexing[otherMovieId]
        sum += sim_scores[otherMovieIndex]*otherMovieRating
        weights += sim_scores[otherMovieIndex]


    weightedAverage = sum/weights
    print("Average Rating : ", weightedAverage)

    print("Thus recommending Movie with a rating of : ", weightedAverage)


def findUserRecommendation(cosine_sim, indexing):
    def parse_MOVIEID_MOVIENAME_FILE(x):
        line = x.split("\t")
        movieId = line[0]
        movieName = line[1]
        return (movieId, [movieName])

    def parse_MOVIEID_USERID_RATINGS_FILE(x):
        line = x.split("\t")
        movieId = line[0]
        userId = line[1]
        rating = line[2]
        return (movieId, [userId, rating])

    recommendMovieToUser(userId=54121, movieId=9, cosine_sim=cosine_sim, indexing=indexing )


if __name__ == '__main__':
    cosine_sim, indexing = findSimilarity(sc)
    findUserRecommendation(cosine_sim, indexing)

