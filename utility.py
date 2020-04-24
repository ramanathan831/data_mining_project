import math
from sklearn.metrics import mean_absolute_error, mean_squared_error


# Calculates the RMSE and MEA
def mae_rmse(filename):
    actual = []
    predicted = []

    fileobject = open(filename)
    for line in fileobject.readlines():
        userId, movieId, actualRating, predictedRating = line.split("\t")
        actual.append(float(actualRating))
        predicted.append(float(predictedRating))

    mae = mean_absolute_error(actual, predicted)
    mse = mean_squared_error(actual, predicted)
    rmse = math.sqrt(mse)

    return mae, rmse


# create movie_list with all ids - movies.txt
# get all unique user list
#for every user:
    # at the user movie list -> already_rated_list
        # for every movie in movie_list:
            # if movie not in already_rated_list:
                # calculate probab
                # tuple.append(movieid, probab)
    # sort (movieid, probab) tuple based on probab
    # get the top 50,40
    # write to file (userId, [topx])