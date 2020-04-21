#python clean_movie_tweetings_dataset.py datasets/movies.txt datasets/ratings.txt datasets/tmdb_metadata.tsv datasets/movies_cleaned.txt datasets/ratings_cleaned.txt datasets/tmdb_metadata_cleaned.tsv
import sys

def main():
    movies_input_file = sys.argv[1]
    ratings_input_file = sys.argv[2]
    tmdb_input_file = sys.argv[3]
    movies_output_file = sys.argv[4]
    ratings_output_file = sys.argv[5]
    tmdb_output_file = sys.argv[6]

    tmdbmovie_ids = []
    tmdb_movie = open(tmdb_input_file,"r")
    tmdb_movie_lines = tmdb_movie.readlines()
    for row in tmdb_movie_lines[1:]:
        tmdbmovie_ids.append(row.split("\t")[5].replace("tt",""))

    file_ptr = open(movies_output_file,"w")
    cleaned_movie_ids = []
    with open(movies_input_file,"r") as f:
        line = f.readline()
        sentences = []
        while line:
            movie_id = line.split("::")[0]
            movie_name = line.split("::")[1].split(" (")[0].replace(")","")
            release_year = line.split("::")[1].split(" (")[1].split(")")[0]
            genre = line.split("::")[2].strip().split("|")
            if int(release_year) >= 2010 and movie_id in tmdbmovie_ids:
                cleaned_movie_ids.append(movie_id)
                file_ptr.write("%s\t%s\t%s\t" % (movie_id,movie_name,release_year))
                file_ptr.flush()
                for ind_genre in genre:
                    file_ptr.write("%s," %(ind_genre))
                    file_ptr.flush()
                file_ptr.write("\n")
                file_ptr.flush()
            line = f.readline()
    file_ptr.close()

    file_ptr = open(movies_output_file,"r")
    cleaned_movies_list = file_ptr.readlines()
    file_ptr.close()
    file_ptr = open(ratings_output_file,"w")

    with open(ratings_input_file,"r") as f:
        line = f.readline()
        while line:
            movie_id = line.split("::")[1]
            user_id = line.split("::")[0]
            rating = line.split("::")[2]
            timestamp = line.split("::")[3]
            if(movie_id in cleaned_movie_ids):
                file_ptr.write("%s\t%s\t%s\t%s" % (movie_id,user_id,rating,timestamp))
                file_ptr.flush()
            line = f.readline()

    file_ptr.close()

    print(len(cleaned_movie_ids))
    intersection_metadata = open(tmdb_output_file,"w")
    intersection_metadata.write(tmdb_movie_lines[0])
    for row in tmdb_movie_lines[1:]:
        if(row.split("\t")[5].replace("tt","") in cleaned_movie_ids):
            intersection_metadata.write(row)

if __name__ == '__main__':
    main()
