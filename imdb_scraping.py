import sys
from imdb import IMDb

def main():
	inpfileptr = open(sys.argv[1],"r")
	# review_op_file = open("imdb_reviews.tsv","a")
	imdb_scrape_file = open("imdb_metadata.tsv", "a")
	line = inpfileptr.readline()
	ia = IMDb()
	# imdb_scrape_file.write("IMDB_ID\tMovie_Name\tGenres\tPlot\tRelease_Date\tKey_Words\tImage_URL\tDirectors\tCast\tWriters\tComposers\tCountry\tLanguage\tProduction_Companies\tMale_Rating\tFemale_Rating\tChildren_Rating\tYoungster_Rating\tMid_Aged_Rating\tOld_People_Rating\n")
	# imdb_scrape_file.flush()
	print("HIHI")
	while line:
		movie_id = line.split("::")[0]
		release_year = line.split("::")[1].split("(")[1].replace(")","")
		if(int(release_year) < 1997):
			line = inpfileptr.readline()
			continue

		movie = ia.get_movie(movie_id)
		print(movie_id)

		movie_name = ""
		if('long imdb title' in movie.keys()):
			movie_name = movie['long imdb title']
			if(movie_name .find("(") != -1):
				movie_name = movie_name.split("(")[0]
		if movie_name == "":
			movie_name = line.split("::")[1]

		poster_web_location = ""
		if('cover url' in movie.keys()):
			poster_web_location = movie['cover url']

		genres = ""
		if('genres') in movie.keys():
			genres_list = movie['genres']
			for genre in genres_list:
				genres += (genre + ",")

		plot = ""
		if('plot') in movie.keys():
			plot = movie['plot']

		# print(sorted(movie.keys()))

		directors = ""
		if('directors' in movie.keys()):
			for director in movie['directors']:
				if 'name' in director.keys():
					directors += (director['name'] + ",")

		cast = ""
		if('cast' in movie.keys()):
			for ind_cast in movie['cast']:
				if 'name' in ind_cast.keys():
					cast += (ind_cast['name'] + ",")

		writers = ""
		if('writers' in movie.keys()):
			for writer in movie['writers']:
				if('name' in writer.keys()):
					writers += (writer['name'] + ",")

		composers = ""
		if('composers' in movie.keys()):
			for composer in movie['composers']:
				if 'composer' in composer.keys():
					composers += (composer['name'] + ",")

		countries = ""
		if('countries' in movie.keys()):
			for country in movie['countries']:
				countries += (country + ",")

		languages = ""
		if('language codes' in movie.keys()):
			for language in movie['language codes']:
				languages += (language + ",")

		production_companies = ""
		if('production companies' in movie.keys()):
			for production_company in movie['production companies']:
				production_companies += (production_company['name'] + ",")

		# print(ia.get_movie_infoset())

		# ia.update(movie, ['reviews'])
		# reviews = movie.get('reviews')
		# if(reviews != None):
		# 	for ind_review in reviews:
		# 		review_op_file.write("%s\t%s\t%s\n" %(movie_id, movie_name, ind_review['content']))
		# 		review_op_file.flush()

		release_date = ""
		ia.update(movie, ['akas'])
		rds = movie.get('raw release dates')
		if(rds != None):
			for release_date_obj in rds:
				release_date = release_date_obj['date']
				break

		ia.update(movie, ['keywords'])
		keywords = ""
		kws = movie.get('keywords')
		if(kws != None):
			for ind_kw in kws:
				keywords += (ind_kw + ",")

		ia.update(movie, ['vote details'])
		ratings_dict = movie.get('demographics')
		male_rating = ""
		female_rating = ""
		children_rating = ""
		youngster_rating = ""
		mid_age_rating = ""
		old_age_rating = ""
		if(ratings_dict != None):
			if('males' in ratings_dict.keys()):
				male_rating = ratings_dict['males']['rating']
			if('females' in ratings_dict.keys()):
				female_rating = ratings_dict['females']['rating']
			if('aged under 18' in ratings_dict.keys()):
				children_rating = ratings_dict['aged under 18']['rating']
			if('aged 18 29' in ratings_dict.keys()):
				youngster_rating = ratings_dict['aged 18 29']['rating']
			if('aged 30 44' in ratings_dict.keys()):
				mid_age_rating = ratings_dict['aged 30 44']['rating']
			if('aged 45 plus' in ratings_dict.keys()):
				old_age_rating = ratings_dict['aged 45 plus']['rating']

		imdb_scrape_file.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" %
								(movie_id, movie_name, genres, plot, release_date, keywords, poster_web_location,
								directors, cast, writers, composers, countries, languages, production_companies,
								male_rating, female_rating, children_rating, youngster_rating, mid_age_rating,
								old_age_rating))
		imdb_scrape_file.flush()

		line = inpfileptr.readline()

if __name__ == '__main__':
	main()
