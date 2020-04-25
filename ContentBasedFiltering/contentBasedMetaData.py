from time import time
from pyspark import SparkContext
import constants as CONSTANTS
from ContentBasedFiltering.moviemovieCosineSimilarity import *
from utility import mae_rmse

sc = SparkContext('local[*]', 'content-based')
sc.setLogLevel("ERROR")

MOVIEID = 'movieId'
RATINGS = 'ratings'


def getUserRatingsRdd(filename):
    def parse(x):
        return int(x[0]), {MOVIEID: int(x[1]), RATINGS: int(x[2])}

    data = sc.textFile(filename)

    rdd_data = data \
        .map(lambda x: x.split("\t"))\
        .filter(lambda x: x[0] != CONSTANTS.HEADER.get(CONSTANTS.USER_ID)) \
        .map(lambda x: parse(x)) \
        .groupByKey() \
        .map(lambda x: (x[0], list(x[1])))

    return rdd_data


def recommendMovieToUser(trainingDataList, userId, movieId, cosine_sim, indexing, userIndexing):
    # Getting the id of the movie for which the user want recommendation
    movieIndex = indexing[str(movieId)]

    # print("Index in Similarity Matrix", movieIndex)
    # Getting all the similar cosine score for that movie
    sim_scores = list(cosine_sim[movieIndex])

    # print("Similarity scores for all other movies with this movie : ", sim_scores)

    # Get only the userId row that we need
    userIndex = userIndexing[userId]
    # A list
    userMovieRatings = trainingDataList[userIndex][1]
    # print("userMovieRatings", userMovieRatings)

    sum = 0
    weights = 0
    for userRatedMovie in userMovieRatings:
        # print("UserRatedMovie", userRatedMovie)
        otherMovieId = userRatedMovie.get(MOVIEID)
        otherMovieRating = userRatedMovie.get(RATINGS)
        otherMovieIndex = indexing[str(otherMovieId)]

        similarity = sim_scores[otherMovieIndex]
        sum += similarity * otherMovieRating
        weights += similarity

    if weights != 0:
        weightedAverage = sum / weights
    else:
        weightedAverage = 5
        # print("Weight is 0", sim_scores)

    # print("Average Rating : ", weightedAverage)
    return weightedAverage


def predictTestUserRatings(cosine_sim, indexing):

    def testing(testingUserData):
        # print("Testing for userData : ", testingUserData)
        userId = testingUserData[0]
        movieRatingList = testingUserData[1]

        result = []
        for movieRating in movieRatingList:
            # print("Movie Rating : ", movieRating)
            actualRating = movieRating.get(RATINGS)
            movieId = movieRating.get(MOVIEID)
            predictedRating = recommendMovieToUser(trainingDataList = trainingDataList,
                                                   userId=userId, movieId=movieId,
                                                   cosine_sim=cosine_sim, indexing=indexing,
                                                   userIndexing = userIndexing)

            result.append((movieId, actualRating, predictedRating))
            # print("Actual Rating : {0}, Predicted Rating : {1}".format(actualRating, predictedRating))

        return (userId, result)

    file = open(CONSTANTS.CONTENT_BASED + CONSTANTS.PREDICTED_RATINGS_FILE, 'w+')

    trainingRdd = getUserRatingsRdd(CONSTANTS.TRAINSET_FILE)
    trainingDataList = trainingRdd.collect()
    # print("Training Data List : ", trainingDataList)

    trainingDataUsers = trainDataRdd.map(lambda userMovieRatings: userMovieRatings[0])\
        .distinct().collect()

    testingRdd = getUserRatingsRdd(CONSTANTS.TESTSET_FILE)
    testDataList = testingRdd.collect()
    # print("Testing RDD : ", testingRdd.take(10))
    # print("Testing Data length : ", len(testDataList))

    indexes = []
    for i in range(0, len(trainingDataUsers)):
        indexes.append(i)

    userIndexing = pd.Series(indexes, trainingDataUsers)
    # print("UserIds  |  Indexes ")
    # print(userIndexing)

    print(testingRdd.count())

    for testingUserData in testingRdd.collect():
        x = testing(testingUserData)
        userId = x[0]
        movieList = x[1]

        for movie in movieList:
            line = tuple([str(userId), str(movie[0]), str(movie[1]), str(movie[2])])
            file.write('\t'.join(line) + '\n')

    file.close()


def train_and_test():
    featuresList = [CONSTANTS.GENRES]
    print(" Considering Features : {0}".format(featuresList))
    cosine_sim, indexing = findSimilarity(sc, featuresList)
    predictTestUserRatings(cosine_sim, indexing)


if __name__ == '__main__':
    startTime = time()
    endTime = time()

    train_and_test()

    mae, rmse, ndcg = mae_rmse(CONSTANTS.CONTENT_BASED + CONSTANTS.PREDICTED_RATINGS_FILE)

    print("Your result took : {0:f}s".format(endTime - startTime))
    print("Your MAE is : {0} \nYour RMSE is : {1} \nYour NDCG is : {2}".format(mae, rmse, ndcg))
