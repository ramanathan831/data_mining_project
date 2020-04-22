#!/usr/bin/env python
# coding: utf-8

# In[3]:


import os
import time

# spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import UserDefinedFunction, explode, desc
from pyspark.sql.types import StringType, ArrayType, IntegerType, StructField, StructType, FloatType
from pyspark.mllib.recommendation import ALS

# Math and ML imports
import math
import numpy as np
import pandas as pd


# In[4]:


spark = SparkSession.builder.appName('CF_Rec').getOrCreate()


# In[5]:


ratings = spark.read.option("delimiter", ":").csv('ratings.dat',inferSchema=True, header=True)


# In[6]:


ratings=ratings.drop('_c1','_c3','_c5','ratingTimestamp')


# In[7]:


ratings.show(3)


# In[8]:


movies = spark.read.option("delimiter", ":").csv('movies.dat',inferSchema=True, header=True)


# In[9]:


movies=movies.drop('_c1','_c3')


# In[10]:


movies.show(3)


# In[11]:


# get spark context
sc = spark.sparkContext


# In[12]:


# load data
movie_rating = sc.textFile('ratings.dat')
# preprocess data -- only need ["userId", "movieId", "rating"]
header = movie_rating.take(1)[0]
rating_data = movie_rating.filter(lambda line: line!=header).map(lambda line: line.split("::")).map(lambda tokens:(int(tokens[0]), int(tokens[1]), float(tokens[2]))).cache()
# check three rows
rating_data.take(3)


# In[13]:


ratings.take(3)


# In[14]:


train, validation, test = rating_data.randomSplit([6, 2, 2], seed=99)
# cache data
train.cache()
validation.cache()
test.cache()


# In[15]:


def train_ALS(train_data, validation_data, num_iters, reg_param, ranks):
    """
    To Find best model's number of latent factors and regularization
    """
    # initial
    min_error = float('inf')
    best_rank = -1
    best_regularization = 0
    best_model = None
    for rank in ranks:
        for reg in reg_param:
            # train ALS model
            model = ALS.train(
                ratings=train_data,    # (userID, productID, rating) tuple
                iterations=num_iters,
                rank=rank,
                lambda_=reg,           # regularization param
                seed=99)
            # make prediction
            valid_data = validation_data.map(lambda p: (p[0], p[1]))
            predictions = model.predictAll(valid_data).map(lambda r: ((r[0], r[1]), r[2]))
            # get the rating result
            ratesAndPreds = validation_data.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
            # get the RMSE
            MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
            error = math.sqrt(MSE)
            print('{} latent factors and regularization = {}: validation RMSE is {}'.format(rank, reg, error))
            if error < min_error:
                min_error = error
                best_rank = rank
                best_regularization = reg
                best_model = model
    print('\nThe best model has {} latent factors and regularization = {}'.format(best_rank, best_regularization))
    return best_model


# In[16]:


# hyper-param config
num_iterations = 10
ranks = [8, 10, 12, 14, 16, 18, 20]
reg_params = [0.001, 0.01, 0.05, 0.1, 0.2]

# Calclate best model using above iterators.
start_time = time.time()
final_model = train_ALS(train, validation, num_iterations, reg_params, ranks)

print ('Total Runtime: {:.2f} seconds'.format(time.time() - start_time))


# In[17]:


# make prediction using test data
test_data = test.map(lambda p: (p[0], p[1]))
predictions = final_model.predictAll(test_data).map(lambda r: ((r[0], r[1]), r[2])) #userId, movieId, Rating
# get the rating result
ratesAndPreds = test.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
# get the RMSE
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
error = math.sqrt(MSE)
print('The test_data RMSE of rating predictions is', round(error, 4))


# In[18]:


ratingPredictions=ratesAndPreds.toDF()


# In[19]:


sqlContext.registerDataFrameAsTable(ratingPredictions, "ratings")
ratingPredictions = sqlContext.sql("SELECT _1 AS UserID_MovieID, _2 as RealRating_PredictedRating from ratings")


# In[20]:


ratingPredictions.head()


# In[21]:


ratingPredictions.show()


# In[22]:


def get_movieId(df_movies, fav_movie_list):
    """
    return all movieId(s) of user's favorite movies
    
    Parameters
    ----------
    df_movies: spark Dataframe, movies data
    
    fav_movie_list: list, user's list of favorite movies
    
    Return
    ------
    movieId_list: list of movieId(s)
    """
    movieId_list = []
    for movie in fav_movie_list:
        movieIds = df_movies.filter(movies.title.like('%{}%'.format(movie))).select('movieId').rdd.map(lambda r: r[0]).collect()
        movieId_list.extend(movieIds)
    return list(set(movieId_list))


def add_new_user_to_data(train_data, movieId_list, spark_context):
    """
    add new rows with new user, user's movie and ratings to
    existing train data

    Parameters
    ----------
    train_data: spark RDD, ratings data
    
    movieId_list: list, list of movieId(s)

    spark_context: Spark Context object
    
    Return
    ------
    new train data with the new user's rows
    """
    # get new user id
    new_id = train_data.map(lambda r: r[0]).max() + 1
    # get max rating
    max_rating = train_data.map(lambda r: r[2]).max()
    # create new user rdd
    user_rows = [(new_id, movieId, max_rating) for movieId in movieId_list]
    new_rdd = spark_context.parallelize(user_rows)
    # return new train data
    return train_data.union(new_rdd)


def get_inference_data(train_data, df_movies, movieId_list):
    """
    return a rdd with the userid and all movies (except ones in movieId_list)

    Parameters
    ----------
    train_data: spark RDD, ratings data

    df_movies: spark Dataframe, movies data
    
    movieId_list: list, list of movieId(s)

    Return
    ------
    inference data: Spark RDD
    """
    # get new user id
    new_id = train_data.map(lambda r: r[0]).max() + 1
    # return inference rdd
    return df_movies.rdd.map(lambda r: r[0]).distinct().filter(lambda x: x not in movieId_list).map(lambda x: (new_id, x))


def make_recommendation(best_model_params, ratings_data, df_movies, 
                        fav_movie_list, n_recommendations, spark_context):
    """
    return top n movie recommendation based on user's input list of favorite movies


    Parameters
    ----------
    best_model_params: dict, {'iterations': iter, 'rank': rank, 'lambda_': reg}

    ratings_data: spark RDD, ratings data

    df_movies: spark Dataframe, movies data

    fav_movie_list: list, user's list of favorite movies

    n_recommendations: int, top n recommendations

    spark_context: Spark Context object

    Return
    ------
    list of top n movie recommendations
    """
    # modify train data by adding new user's rows
    movieId_list = get_movieId(df_movies, fav_movie_list)
    train_data = add_new_user_to_data(ratings_data, movieId_list, spark_context)
    
    # train best ALS
    model = ALS.train(
        ratings=train_data,
        iterations=best_model_params.get('iterations', None),
        rank=best_model_params.get('rank', None),
        lambda_=best_model_params.get('lambda_', None),
        seed=99)
    
    # get inference rdd
    inference_rdd = get_inference_data(ratings_data, df_movies, movieId_list)
    
    # inference
    predictions = model.predictAll(inference_rdd).map(lambda r: (r[1], r[2]))
    
    # get top n movieId
    topn_rows = predictions.sortBy(lambda r: r[1], ascending=False).take(n_recommendations)
    topn_ids = [r[0] for r in topn_rows]
    
    # return movie titles
    return df_movies.filter(movies.movieId.isin(topn_ids)).select('title').rdd.map(lambda r: r[0]).collect()


# In[25]:



# favorite movies
my_favorite_movies = ['City of Life']

# get recommendations
recommends = make_recommendation(
    best_model_params={'iterations': 10, 'rank': 8, 'lambda_': 0.2}, 
    ratings_data=rating_data, 
    df_movies=movies, 
    fav_movie_list=my_favorite_movies, 
    n_recommendations=10, 
    spark_context=sc)

print('Recommendations for {}:'.format(my_favorite_movies[0]))
for i, title in enumerate(recommends):
    print('{0}: {1}'.format(i+1, title))


# #Predictions dataframe from column of lists to mulitple columns

# In[27]:


ratingPredictions=ratingPredictions.toPandas()


# In[28]:


print(ratingPredictions)


# In[31]:


ratingPredictions[['UserId','MovieId']] = pd.DataFrame(ratingPredictions.UserID_MovieID.values.tolist(), index= ratingPredictions.index)


# In[32]:


ratingPredictions[['realRating','predictedRating']] = pd.DataFrame(ratingPredictions.RealRating_PredictedRating.values.tolist(), index= ratingPredictions.index)


# In[33]:


print(ratingPredictions)


# In[35]:


ratingPredictions=ratingPredictions.drop(['UserID_MovieID', 'RealRating_PredictedRating'], axis=1)


# In[36]:


print(ratingPredictions)


# In[37]:


from pyspark.sql.types import StringType, IntegerType, StructField, StructType, FloatType


# In[38]:


dataSchema = StructType([ StructField("UserId", IntegerType(), True)
                       ,StructField("MovieId", IntegerType(), True)\

                       ,StructField("realRating", FloatType(), True)\

                       ,StructField("predictedRating", FloatType(), True)])


# In[40]:


ratingPredictions = spark.createDataFrame(ratingPredictions,schema=dataSchema)


# In[41]:


type(ratingPredictions)


# In[42]:


ratingPredictions.show()


# In[43]:


ratingPredictions.orderBy('predictedRating', ascending=False).show()


# In[115]:


sc.stop()
spark.stop()


