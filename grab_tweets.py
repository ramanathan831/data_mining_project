import sys
import TweetManager
import Tweet
import TweetCriteria
import datetime

def main():

    movies_file = open(sys.argv[1],"r")
    tweets_file = open(sys.argv[2], "w")
    num_tweets = int(sys.argv[3])

    line = movies_file.readline()
    while line:
        line = movies_file.readline()
        movie_name = line.split("\t")[6]
        movie_name_without_spaces = line.split("\t")[6].replace(" ","").replace(":","")
        movie_name_with_hash_tags = "#" + line.split("\t")[6].replace(" ","").replace(":","")


        release_date = line.split("\t")[13]
        start_date = datetime.datetime.strptime(release_date[2:], "%y-%m-%d")
        end_date = start_date + datetime.timedelta(days=10)

        start_date = str(start_date).split(" ")[0]
        end_date = str(end_date).split(" ")[0]
        print(movie_name)
        print(movie_name_without_spaces)
        print(movie_name_with_hash_tags)

        tweetCriteria = TweetCriteria.TweetCriteria().setQuerySearch(movie_name).setSince(start_date).setUntil(end_date).setMaxTweets(num_tweets)
        for tweet in TweetManager.TweetManager.getTweets(tweetCriteria):
            tweets_file.write("%s\t%s\n" %(movie_name,tweet.text))


if __name__ == '__main__':
    main()
