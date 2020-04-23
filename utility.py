import math
from sklearn.metrics import mean_absolute_error, mean_squared_error


# Calculates the RMSE and MEA
def mae_rmse(actual, predicted):
    mae = mean_absolute_error(actual, predicted)
    mse = mean_squared_error(actual, predicted)
    rmse = math.sqrt(mse)

    return mae, rmse