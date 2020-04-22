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


# In[3]:


ratings = spark.read.option("delimiter", "\t").csv('Data/ratings.txt',inferSchema=True, header=True)


# In[4]:


ratings=ratings.drop('ratingTimestamp')


# In[5]:


ratings=ratings.select("userId","movieId","rating")


# In[92]:


# ratings.show(3)


# In[8]:


movies = spark.read.option("delimiter", "\t").csv('Data/moviesCF.txt',inferSchema=True, header=True)


# In[91]:


# movies.show(3)


# In[10]:


# get spark context
sc = spark.sparkContext


# In[29]:


# load full ratings data
movie_rating = sc.textFile('Data/ratings.txt')
#Create ratingsFullData RDD
header = movie_rating.take(1)[0]
rating_data = movie_rating.filter(lambda line: line!=header).map(lambda line: line.split("\t")).map(lambda tokens: (int(tokens[0]), int(tokens[1]), float(tokens[2]))).cache()#sparkcontext gets the file line by line.Two map functions one after the other to split the data into tokens.
# check three rows
rating_data.take(3)


# In[13]:


# load train data
movie_train_rating = sc.textFile('Data/train_set.txt')
#Create trainingData RDD
header = movie_train_rating.take(1)[0]
rating_traindata = movie_train_rating.filter(lambda line: line!=header).map(lambda line: line.split("\t")).map(lambda tokens: (int(tokens[0]), int(tokens[1]), float(tokens[2]))).cache()#sparkcontext gets the file line by line.Two map functions one after the other to split the data into tokens.
# check three rows
rating_traindata.take(3)


# In[15]:


# load test data
movie_test_rating = sc.textFile('Data/test_set.txt')
# create testData RDD
header = movie_test_rating.take(1)[0]
rating_testdata = movie_test_rating.filter(lambda line: line!=header).map(lambda line: line.split("\t")).map(lambda tokens: (int(tokens[0]), int(tokens[1]), float(tokens[2]))).cache()
# check three rows
rating_testdata.take(3)


# In[16]:


#Data split for traning and validation
test= rating_testdata
train, validation = rating_traindata.randomSplit([8, 2], seed=99)
#cache data
train.cache()
validation.cache()
test.cache()


# In[17]:


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
            # training ALS model with each set of input params as per the iterables - ranks,reg_param
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
    return best_model #the one with rank,reg-param that gives minimum RMSE of all


# In[18]:


#Tuning Hyper-Params:
num_iterations = 10
ranks = [8, 10, 12, 14, 16, 18, 20]
reg_params = [0.001, 0.01, 0.05, 0.1, 0.2]

# check on ranks and reg-params and select best model
start_time = time.time()
final_model = train_ALS(train, validation, num_iterations, reg_params, ranks) #returns best model

print ('Total Runtime: {:.2f} seconds'.format(time.time() - start_time))


# In[19]:


# make prediction using test data
test_data = test.map(lambda p: (p[0], p[1]))
predictions = final_model.predictAll(test_data).map(lambda r: ((r[0], r[1]), r[2])) #userId, movieId, Rating
# get the rating result
ratesAndPreds = test.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
# get the RMSE
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
error = math.sqrt(MSE)
print('RMSE of predicted ratings and real ratings for test_set.txt is', round(error, 4))


# In[24]:


ratingPredictions=ratesAndPreds.toDF()


# In[25]:


sqlContext.registerDataFrameAsTable(ratingPredictions, "ratings")
ratingPredictions = sqlContext.sql("SELECT _1 AS UserID_MovieID, _2 as RealRating_PredictedRating from ratings")


# In[26]:


ratingPredictions.head()


# In[27]:


ratingPredictions.show()


# In[31]:


def get_movieId(df_movies, fav_movie_list):
    """
    Return movieId's of new user's - fav movie list.
    
    Arguments:
    ***********
    df_movies: spark Dataframe, movies data
    fav_movie_list: list, user's list of favorite movies
    
    Output:
    *******
    movieId_list: list of movieId(s)
    """
    movieId_list = []
    for movie in fav_movie_list:
        movieIds = df_movies.filter(movies.title.like('%{}%'.format(movie))).select('movieId').rdd.map(lambda r: r[0]).collect()
        movieId_list.extend(movieIds)
        #.like() is spark method to retrieve the movies from movies-DF with title having a match with the word from the list.
    return list(set(movieId_list))


def add_new_user_to_data(train_data, movieId_list, spark_context):
    """
    add new rows with new user, user's movie and ratings to
    existing train data

    Arguments:
    **********
    train_data: type:spark RDD, content:complete ratings data
    movieId_list: type:list, content:list of movieId(s)
    spark_context: type:Spark Context object
    
    Output:
    *******
    new train data with the new user's rows (we add this new user to train data and calculate the user-movie matrix to find the recommendations of the user. We pick the top ten (or as requested) predicted ratings for the user)
    """
    # get new user id
    new_id = train_data.map(lambda r: r[0]).max() + 1 #creating a newId to the user by adding 1 to max avaliable in the actual dataset.
    # get max rating
    max_rating = train_data.map(lambda r: r[2]).max() #getting max rating from the actual rating dataset
    # create new user rdd
    user_rows = [(new_id, movieId, max_rating) for movieId in movieId_list]
    new_rdd = spark_context.parallelize(user_rows)
    # join the new user data to overall ratings data and return new train data
    return train_data.union(new_rdd)


def get_inference_data(train_data, df_movies, movieId_list):
    """
    return a rdd with the userid and all movies (except ones in movieId_list)

    Arguments:
    **********
    train_data: type:spark RDD, content:complete ratings data
    df_movies: type:spark Dataframe, content:movies dataframe
    movieId_list: type:list, content:list of movieId(s)

    Output:
    *******
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
    
    Arguments:
    ***********
    best_model_params: type:dict, content:{'iterations': iter, 'rank': rank, 'lambda_': reg}
    ratings_data: type:spark RDD, content: full ratings data
    df_movies: type:spark Dataframe, content: movies data
    fav_movie_list: type:list, content: user's list of favorite movies
    n_recommendations: type:int, content: top n recommendations
    spark_context: type:Spark content: Context object

    Output:
    *******
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


# In[90]:



# favorite movies
my_favorite_movies = ['Iron Man','lidice']

# get recommendations
recommends = make_recommendation(
    best_model_params={'iterations': 10, 'rank': 8, 'lambda_': 0.2}, 
    ratings_data=rating_data, 
    df_movies=movies, 
    fav_movie_list=my_favorite_movies, 
    n_recommendations=10, 
    spark_context=sc)

print('Recommendations for the movies:')
for i in my_favorite_movies:
    print(i, end="|")
print("\n")
for i, title in enumerate(recommends):
    print('{0}: {1}'.format(i+1, title))


# #Predictions dataframe from column of lists to mulitple columns

# In[60]:


ratingPredictions=ratingPredictions.toPandas()


# In[76]:


# print(ratingPredictions)


# In[62]:


ratingPredictions[['UserId','MovieId']] = pd.DataFrame(ratingPredictions.UserID_MovieID.values.tolist(), index= ratingPredictions.index)


# In[63]:


ratingPredictions[['realRating','predictedRating']] = pd.DataFrame(ratingPredictions.RealRating_PredictedRating.values.tolist(), index= ratingPredictions.index)


# In[57]:


# print(ratingPredictions)


# In[64]:


ratingPredictions=ratingPredictions.drop(['UserID_MovieID', 'RealRating_PredictedRating'], axis=1)


# In[66]:


# print(ratingPredictions)


# In[67]:


from pyspark.sql.types import StringType, IntegerType, StructField, StructType, FloatType


# In[68]:


dataSchema = StructType([ StructField("UserId", IntegerType(), True)
                       ,StructField("MovieId", IntegerType(), True)\

                       ,StructField("realRating", FloatType(), True)\

                       ,StructField("predictedRating", FloatType(), True)])


# In[69]:


ratingPredictions = spark.createDataFrame(ratingPredictions,schema=dataSchema)


# In[70]:


type(ratingPredictions)


# In[71]:


ratingPredictions=ratingPredictions.select("UserId","MovieId","realRating","predictedRating")


# In[77]:


# ratingPredictions.orderBy('predictedRating', ascending=False).show()


# In[73]:


from pyspark.ml.evaluation import RegressionEvaluator


# In[74]:


evaluator = RegressionEvaluator(metricName="rmse",
                                            labelCol="realRating",
                                            predictionCol="predictedRating")
rmse = evaluator.evaluate(ratingPredictions)


# In[75]:


print('validation RMSE on test data {}'.format(rmse))


# In[42]:


sc.stop()
spark.stop()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




