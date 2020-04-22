import urllib.request, urllib.parse, urllib.error,urllib.request,urllib.error,urllib.parse,json,re,datetime,sys,http.cookiejar
import Tweet
import nltk
from pyquery import PyQuery
import string
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import pandas as pd
import csv
import re #regular expression
from textblob import TextBlob
import string
import preprocessor as p
from autocorrect import spell
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import numpy as np

def remove_pattern(input_txt, pattern):
    r = re.findall(pattern, input_txt)
    for i in r:
        input_txt = re.sub(i, '', input_txt)

    return input_txt

def emoji(string):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', string)

def clean(data):
	# stop_words = set(stopwords.words('english'))

	# after tweepy preprocessing the colon left remain after removing mentions
	# or RT sign in the beginning of the data
	data = np.vectorize(remove_pattern)(data, "@[\w]*")
	data = str(data).replace("[^a-zA-Z#]", " ")
	# data = data.apply(lambda x: ' '.join([w for w in x.split() if len(w)>3]))
	data = re.sub(r':', '', data)
	data = re.sub(r"\d", "", data)
	# replace consecutive non-ASCII characters with a space
	data = re.sub(r'[^\x00-\x7F]+', ' ', data)
	# remove emojis from data
	data = emoji(data)
	lst = []
	for w in data.split():
		if(w != None):
			lst.append(spell(w.lower()))
	data = ' '.join(lst)
	word_tokens = word_tokenize(data)
	# exit(1)
	# filter using NLTK library append it to a string
	# filtered_data = [w for w in word_tokens if not w in stop_words]
	filtered_data = []

	# looping through conditions
	for w in word_tokens:
		# check tokens against stop words , emoticons and punctuations
		# if w not in emoticons and w not in string.punctuation:
			filtered_data.append(w)
	return ' '.join(filtered_data)
	# return filtered_data


class TweetManager:



	def __init__(self):
		pass


	@staticmethod
	def getTweets(tweetCriteria, receiveBuffer=None, bufferLength=100, proxy=None):
		refreshCursor = ''

		results = []
		resultsAux = []
		cookieJar = http.cookiejar.CookieJar()

		active = True

		while active:
			json = TweetManager.getJsonReponse(tweetCriteria, refreshCursor, cookieJar, proxy)
			if len(json['items_html'].strip()) == 0:
				break

			refreshCursor = json['min_position']
			scrapedTweets = PyQuery(json['items_html'])
			#Remove incomplete tweets withheld by Twitter Guidelines
			scrapedTweets.remove('div.withheld-tweet')
			tweets = scrapedTweets('div.js-stream-tweet')

			if len(tweets) == 0:
				break

			for tweetHTML in tweets:
				tweetPQ = PyQuery(tweetHTML)
				tweet = Tweet.Tweet()

				usernameTweet = tweetPQ("span.username.js-action-profile-name b").text()
				txt = re.sub(r"\s+", " ", tweetPQ("p.js-tweet-text").text().replace('# ', '#').replace('@ ', '@'))
				retweets = int(tweetPQ("span.ProfileTweet-action--retweet span.ProfileTweet-actionCount").attr("data-tweet-stat-count").replace(",", ""))
				favorites = int(tweetPQ("span.ProfileTweet-action--favorite span.ProfileTweet-actionCount").attr("data-tweet-stat-count").replace(",", ""))
				dateSec = int(tweetPQ("small.time span.js-short-timestamp").attr("data-time"))
				id = tweetPQ.attr("data-tweet-id")
				permalink = tweetPQ.attr("data-permalink-path")
				user_id = int(tweetPQ("a.js-user-profile-link").attr("data-user-id"))
				tweet.language = tweetPQ("p.js-tweet-text").attr("lang")
				geo = ''
				geoSpan = tweetPQ('span.Tweet-geo')
				if len(geoSpan) > 0:
					geo = geoSpan.attr('title')
				urls = []
				for link in tweetPQ("a"):
					try:
						urls.append((link.attrib["data-expanded-url"]))
					except KeyError:
						pass
				tweet.id = id
				tweet.permalink = 'https://twitter.com' + permalink
				tweet.username = usernameTweet

				tweet.text = txt
				tweet.date = datetime.datetime.fromtimestamp(dateSec)
				tweet.formatted_date = datetime.datetime.fromtimestamp(dateSec).strftime("%a %b %d %X +0000 %Y")
				tweet.retweets = retweets
				tweet.favorites = favorites
				tweet.mentions = " ".join(re.compile('(@\\w*)').findall(tweet.text))
				tweet.hashtags = " ".join(re.compile('(#\\w*)').findall(tweet.text))
				tweet.geo = geo
				tweet.urls = ",".join(urls)
				tweet.author_id = user_id
				if(tweet.language == "en" and len(tweet.text)>120 and tweet.text.find("...") == -1):
					filtered_tweet = p.clean(tweet.text)
					filtered_tweet = clean(filtered_tweet)
					if(len(filtered_tweet) > 100):
						tweet.text = filtered_tweet
						results.append(tweet)
						resultsAux.append(tweet)

				if receiveBuffer and len(resultsAux) >= bufferLength:
					receiveBuffer(resultsAux)
					resultsAux = []

				if tweetCriteria.maxTweets > 0 and len(results) >= tweetCriteria.maxTweets:
					active = False
					break


		if receiveBuffer and len(resultsAux) > 0:
			receiveBuffer(resultsAux)

		return results

	@staticmethod
	def getJsonReponse(tweetCriteria, refreshCursor, cookieJar, proxy):
		url = "https://twitter.com/i/search/timeline?f=tweets&q=%s&src=typd&%smax_position=%s"

		urlGetData = ''
		if hasattr(tweetCriteria, 'username'):
			urlGetData += ' from:' + tweetCriteria.username

		if hasattr(tweetCriteria, 'since'):
			urlGetData += ' since:' + tweetCriteria.since

		if hasattr(tweetCriteria, 'until'):
			urlGetData += ' until:' + tweetCriteria.until

		if hasattr(tweetCriteria, 'querySearch'):
			urlGetData += ' ' + tweetCriteria.querySearch

		if hasattr(tweetCriteria, 'lang'):
			urlLang = 'lang=' + tweetCriteria.lang + '&'
		else:
			urlLang = ''
		url = url % (urllib.parse.quote(urlGetData), urlLang, refreshCursor)
		#print(url)

		headers = [
			('Host', "twitter.com"),
			('User-Agent', "Mozilla/5.0 (Windows NT 6.1; Win64; x64)"),
			('Accept', "application/json, text/javascript, */*; q=0.01"),
			('Accept-Language', "de,en-US;q=0.7,en;q=0.3"),
			('X-Requested-With', "XMLHttpRequest"),
			('Referer', url),
			('Connection', "keep-alive")
		]

		if proxy:
			opener = urllib.request.build_opener(urllib.request.ProxyHandler({'http': proxy, 'https': proxy}), urllib.request.HTTPCookieProcessor(cookieJar))
		else:
			opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookieJar))
		opener.addheaders = headers

		try:
			response = opener.open(url)
			jsonResponse = response.read()
		except:
			#print("Twitter weird response. Try to see on browser: ", url)
			print("Twitter weird response. Try to see on browser: https://twitter.com/search?q=%s&src=typd" % urllib.parse.quote(urlGetData))
			print("Unexpected error:", sys.exc_info()[0])
			sys.exit()
			return

		dataJson = json.loads(jsonResponse.decode())

		return dataJson
