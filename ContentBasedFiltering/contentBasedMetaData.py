from pyspark import SparkContext
from sklearn.metrics import mean_squared_error, mean_absolute_error

import constants as CONSTANTS

from ContentBasedFiltering.moviemovieCosineSimilarity import *

# The following features are present in the dataset
# weights for each feature
from utility import mae_rmse

sc = SparkContext('local[*]', 'content-based')
sc.setLogLevel("ERROR")

MOVIEID = 'movieId'
RATINGS = 'ratings'


def getUserRatingsRdd(filename):

    def parse(x):
        x = x.split("\t")
        return int(x[0]), {MOVIEID: x[1], RATINGS: int(x[2])}

    data = sc.textFile(filename)

    # .filter(lambda x: x[0] != CONSTANTS.HEADER.get(CONSTANTS.USER_ID))\
    rdd_data = data\
        .map(lambda x: parse(x))\
        .groupByKey()\
        .map(lambda x: (x[0], list(x[1])))

    return rdd_data


def recommendMovieToUser(rdd, userId, movieId, cosine_sim, indexing):
    #print("User Movie Ratings : ", rdd.take(10))

    # Getting the id of the movie for which the user want recommendation
    movieIndex = indexing[movieId]
    #print("Index in Similarity Matrix", movieIndex)

    # Getting all the similar cosine score for that movie
    sim_scores = list(cosine_sim[movieIndex])
    #print("Similarity scores for all other movies with this movie : ", sim_scores)


    def parseUserRatings(x):
        if userId == x[0]:
            return True
        else:
            return False

    # Get only the userId row that we need
    userMovieRatings = rdd.filter(lambda x: parseUserRatings(x)).collect()[0][1]
    #print("userMovieRatings", userMovieRatings)

    sum = 0
    weights = 0
    for userRatedMovie in userMovieRatings:
        otherMovieId = userRatedMovie.get(MOVIEID)
        otherMovieRating = userRatedMovie.get(RATINGS)

        if len(otherMovieId) == 6:
            otherMovieId = '0' + str(otherMovieId)

        otherMovieIndex = indexing[otherMovieId]
        sum += sim_scores[otherMovieIndex]*otherMovieRating
        weights += sim_scores[otherMovieIndex]

    weightedAverage = sum/weights
    # print("Average Rating : ", weightedAverage)
    return weightedAverage


def findUserRecommendation(cosine_sim, indexing):
    # def parse_MOVIEID_MOVIENAME_FILE(x):
    #     line = x.split("\t")
    #     movieId = line[0]
    #     movieName = line[1]
    #     return (movieId, [movieName])
    #
    # def parse_MOVIEID_USERID_RATINGS_FILE(x):
    #     line = x.split("\t")
    #     movieId = line[0]
    #     userId = line[1]
    #     rating = line[2]
    #     return (movieId, [userId, rating])
    trainingRdd = getUserRatingsRdd(CONSTANTS.TRAINSET_FILE)
    testingRdd = getUserRatingsRdd(CONSTANTS.TESTSET_FILE)
    testDataList = testingRdd.collect()
    print("Testing Data length : ", len(testDataList))

    count = 0
    predictedRatingsList = []
    actualRatingsList = []

    testingRdd.map(lambda testingUserData: ).
    for testingUserData in testDataList:
        userId = testingUserData[0]
        movieRatingList = testingUserData[1]
        for movieRating in movieRatingList:
            actualRating = movieRating.get(RATINGS)
            movieId = movieRating.get(MOVIEID)
            predictedRating = recommendMovieToUser(userId=userId, movieId=movieId, cosine_sim=cosine_sim, indexing=indexing, rdd=trainingRdd)

            actualRatingsList.append(actualRating)
            predictedRatingsList.append(predictedRating)

            print("Actual Rating : {0}, Predicted Rating : {1}".format(actualRating, predictedRating))

            count+=1
            # print("Count : ", count)

    return mae_rmse(actualRatingsList, predictedRatingsList)


if __name__ == '__main__':
    cosine_sim, indexing = findSimilarity(sc)
    trainDataRdd = getUserRatingsRdd(CONSTANTS.TRAINSET_FILE)
    mae, rmse = findUserRecommendation(cosine_sim, indexing)
    print("Your MAE is : {0} and Your RMSE is : {1}".format(mae, rmse))
