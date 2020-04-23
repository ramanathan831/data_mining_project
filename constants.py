DATASET_DIR = "Datasets"
DATA = "Data"


MOVIE_METADATA_FILE = DATASET_DIR + "/tmdb_metadata_cleaned.tsv"
MOVIEID_MOVIENAME_FILE = DATASET_DIR + "/movies_cleaned.txt"
MOVIEID_USERID_RATINGS_FILE = DATA + "/ratings.txt"
TMDB_MOVIE_METADATA_CLEANED = DATASET_DIR + '/tmdb_metadata_cleaned.tsv'

TRAINSET_FILE = DATASET_DIR + '/train_set.txt'
TESTSET_FILE = DATASET_DIR + '/test_set.txt'

USER_ID = 'userId'

HEADER = {
   USER_ID: 'userId'
}

IMDB_ID = 'imdbId'
TMDB_ID = 'tmdbId'
MOVIE = 'movie'
GENRE_ID = 'genreIDList'
CAST = 'castList'
ADULT = 'isAdult'

METADATA_COLUMNS_MAP = {
        TMDB_ID: 'TMDB_ID',
        MOVIE: 'Movie_Name',
        ADULT: 'Adult',
        GENRE_ID: 'Genre_ID',
        IMDB_ID: 'IMDB_ID',
        'Language': 'Language',
        'Production_Country_Names': 'Production_Country_Names',
        'Director': 'Director',
        'Assistant_Director': 'Assistant_Director',
        CAST: 'Cast'
}


