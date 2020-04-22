DATASET_DIR = "Datasets"
MOVIE_METADATA_FILE = DATASET_DIR + "/tmdb_metadata_cleaned.tsv"
USER_MOVIE_TMDBID_RATINGS_FILE = DATASET_DIR + "/ratings_cleaned.txt"
MOVIEID_MOVIENAME_FILE = DATASET_DIR + "/movies_cleaned.txt"
MOVIEID_USERID_RATINGS_FILE = DATASET_DIR + "/ratings_cleaned.txt"
TMDB_MOVIE_METADATA_CLEANED = DATASET_DIR + '/tmdb_metadata_cleaned.tsv'

HEADER = {
    'userId': 'userId'
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

