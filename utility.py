import math
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np

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
    ndcg = find_ndcg(actual, predicted)
    return mae, rmse, ndcg


def find_dcg(element_list):
    """
    Discounted Cumulative Gain (DCG)
    The definition of DCG can be found in this paper:
        Azzah Al-Maskari, Mark Sanderson, and Paul Clough. 2007.
        "The relationship between IR effectiveness measures and user satisfaction."
    Parameters:
        element_list - a list of ranks Ex: [5,4,2,2,1]
    Returns:
        score
    """
    score = 0.0
    for order, rank in enumerate(element_list):
        score += float(rank)/math.log((order+2),2)
    return score


def find_ndcg(reference, hypothesis):
    """
    Normalized Discounted Cumulative Gain (nDCG)
    Normalized version of DCG:
        nDCG = DCG(hypothesis)/DCG(reference)
    Parameters:
        reference   - a gold standard (perfect) ordering Ex: [5,4,3,2,1]
        hypothesis  - a proposed ordering Ex: [5,2,2,3,1]
    Returns:
        ndcg_score  - normalized score
    """

    return find_dcg(hypothesis)/find_dcg(reference)