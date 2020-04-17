#python3 common_films_btw_tmdb_and_movie_tweetings.py final_structured_general_metadata.tsv datasets/movies_cleaned.txt datasets/ratings_cleaned.txt
import sys
import ntpath

def main():
    tmdbfile = open(sys.argv[1],"r")
    movie_tweetings_movies_file = open(sys.argv[2],"r")
    movie_tweetings_ratings_file = open(sys.argv[3],"r")
    movie_tweetings_movies_opfile = open(sys.argv[2].replace(ntpath.basename(sys.argv[2]),"final_" + ntpath.basename(sys.argv[2])),"w")
    movie_tweetings_ratings_opfile = open(sys.argv[3].replace(ntpath.basename(sys.argv[3]),"final_" + ntpath.basename(sys.argv[3])),"w")

    tmdblist = []
    tmdbline = tmdbfile.readline()
    while tmdbline:
        tmdbline = tmdbfile.readline()
        if(tmdbline.find("\t") != -1):
            tmdbmovie_name = tmdbline.split("\t")[6]
            tmdblist.append(tmdbmovie_name.lower())
    tmdbfile.close()

    movie_tweetings_line = movie_tweetings_movies_file.readline()
    cleaned_movie_ids = []
    intersecting_movie_names = []
    while movie_tweetings_line:
        movie_tweetings_movie = movie_tweetings_line.split("\t")[1]
        if(movie_tweetings_movie.lower() in tmdblist):
            cleaned_movie_ids.append(movie_tweetings_line.split("\t")[0])
            movie_tweetings_movies_opfile.write(movie_tweetings_line)
            intersecting_movie_names.append(movie_tweetings_movie.lower())
        movie_tweetings_line = movie_tweetings_movies_file.readline()

    line = movie_tweetings_ratings_file.readline()
    while line:
        movie_id = line.split("\t")[0]
        if(movie_id in cleaned_movie_ids):
            movie_tweetings_ratings_opfile.write(line)
            movie_tweetings_ratings_opfile.flush()
        line = movie_tweetings_ratings_file.readline()

    movie_tweetings_movies_file.close()
    movie_tweetings_ratings_file.close()
    movie_tweetings_movies_opfile.close()
    movie_tweetings_ratings_opfile.close()

    tmdbfile = open(sys.argv[1],"r")
    tmdbfile_out = open("intersection_" + sys.argv[1],"w")
    tmdbline = tmdbfile.readline()
    while tmdbline:
        tmdbline = tmdbfile.readline()
        if(tmdbline.find("\t") != -1):
            tmdbmovie_name = tmdbline.split("\t")[6]
            if(tmdbmovie_name.lower() in intersecting_movie_names):
                tmdbfile_out.write(tmdbline)
    tmdbfile_out.close()
    tmdbfile.close()

if __name__ == '__main__':
    main()
