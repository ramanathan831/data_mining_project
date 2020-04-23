from matplotlib import pyplot as plt

userMovieList = [1, 2, 3, 4]
cfRating = [1, 4, 9, 16]
contentRating = [2, 3, 12, 15]
sentimentRating = [14, 3, 2, 12]
trueRating = [12, 5, 7, 8]


plt.plot(userMovieList, cfRating, label='COLLAB')
plt.plot(userMovieList, contentRating, label='CONTENT BASED')
plt.plot(userMovieList, sentimentRating, label='SENTIMENT')
plt.plot(userMovieList, trueRating, label='ACTUAL')
plt.legend()
plt.show()