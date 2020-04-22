#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


# In[2]:


spark = SparkSession.builder.appName('CF_Rec').getOrCreate()


# In[12]:


ratings = spark.read.option("delimiter", "\t").csv('Data/ratings.txt',inferSchema=True, header=True)


# In[5]:


# ratings = spark.read.option("delimiter", ":").csv('ratings.dat',inferSchema=True, header=True)


# In[13]:


ratings=ratings.drop('ratingTimestamp')


# In[14]:


ratings=ratings.select("userId","movieId","rating")


# In[15]:


ratings.show(3)


# In[7]:


movies = spark.read.option("delimiter", "\t").csv('Data/movies.txt',inferSchema=True, header=True)


# In[8]:


# movies = spark.read.option("delimiter", ":").csv('movies.dat',inferSchema=True, header=True)


# In[9]:


#movies=movies.drop('_c1','_c3')


# In[8]:


movies.show(3)


# In[9]:


# get spark context
sc = spark.sparkContext


# In[16]:


# load data
movie_rating = sc.textFile('Data/ratings.txt')
# preprocess data -- only need ["userId", "movieId", "rating"]
header = movie_rating.take(1)[0]
rating_data = movie_rating     .filter(lambda line: line!=header).map(lambda line: line.split("\t")).map(lambda tokens: (int(tokens[0]), int(tokens[1]), float(tokens[2])))     .cache()
# check three rows
rating_data.take(3)


# In[17]:


ratings.take(3)


# In[18]:


train, validation, test = rating_data.randomSplit([6, 2, 2], seed=99)
# cache data
train.cache()
validation.cache()
test.cache()


# In[19]:


def train_ALS(train_data, validation_data, num_iters, reg_param, ranks):
    """
    Grid Search Function to select the best model based on RMSE of hold-out data
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


# In[20]:


# hyper-param config
num_iterations = 10
ranks = [8, 10, 12, 14, 16, 18, 20]
reg_params = [0.001, 0.01, 0.05, 0.1, 0.2]

# grid search and select best model
start_time = time.time()
final_model = train_ALS(train, validation, num_iterations, reg_params, ranks)

print ('Total Runtime: {:.2f} seconds'.format(time.time() - start_time))


# In[21]:


# make prediction using test data
test_data = test.map(lambda p: (p[0], p[1]))
predictions = final_model.predictAll(test_data).map(lambda r: ((r[0], r[1]), r[2])) #userId, movieId, Rating
# get the rating result
ratesAndPreds = test.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
# get the RMSE
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
error = math.sqrt(MSE)
print('The out-of-sample RMSE of rating predictions is', round(error, 4))


# In[22]:


ratingPredictions=ratesAndPreds.toDF()


# In[23]:


sqlContext.registerDataFrameAsTable(ratingPredictions, "ratings")
ratingPredictions = sqlContext.sql("SELECT _1 AS UserID_MovieID, _2 as RealRating_PredictedRating from ratings")


# In[24]:


ratingPredictions.head()


# In[25]:


ratingPredictions.show()


# In[26]:


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


# In[40]:


movies.show()
movies=movies.drop("Year")
movies.show()


# In[27]:



# favorite movies
my_favorite_movies = ['The Wicker Tree']

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

# In[28]:


ratingPredictions=ratingPredictions.toPandas()


# In[29]:


print(ratingPredictions)


# In[30]:


ratingPredictions[['UserId','MovieId']] = pd.DataFrame(ratingPredictions.UserID_MovieID.values.tolist(), index= ratingPredictions.index)


# In[31]:


ratingPredictions[['realRating','predictedRating']] = pd.DataFrame(ratingPredictions.RealRating_PredictedRating.values.tolist(), index= ratingPredictions.index)


# In[32]:


print(ratingPredictions)


# In[33]:


ratingPredictions=ratingPredictions.drop(['UserID_MovieID', 'RealRating_PredictedRating'], axis=1)


# In[34]:


print(ratingPredictions)


# In[35]:


from pyspark.sql.types import StringType, IntegerType, StructField, StructType, FloatType


# In[36]:


dataSchema = StructType([ StructField("UserId", IntegerType(), True)
                       ,StructField("MovieId", IntegerType(), True)\

                       ,StructField("realRating", FloatType(), True)\

                       ,StructField("predictedRating", FloatType(), True)])


# In[37]:


ratingPredictions = spark.createDataFrame(ratingPredictions,schema=dataSchema)


# In[38]:


type(ratingPredictions)


# In[39]:


ratingPredictions=ratingPredictions.select("UserId","MovieId","realRating","predictedRating")


# In[35]:


ratingPredictions.orderBy('predictedRating', ascending=False).show()


# In[ ]:





# In[ ]:





# In[42]:


sc.stop()
spark.stop()





