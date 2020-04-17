#python clean_movie_tweetings_dataset.py datasets/movies.txt datasets/ratings.txt datasets/movies_cleaned.txt datasets/ratings_cleaned.txt
import sys

def main():
    movies_input_file = sys.argv[1]
    ratings_input_file = sys.argv[2]
    movies_output_file = sys.argv[3]
    ratings_output_file = sys.argv[4]

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
            if int(release_year) >= 2010:
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
if __name__ == '__main__':
    main()
