import sys

def main():
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    file_ptr = open(output_file,"w")

    with open(input_file,"r") as f:
        line = f.readline()
        sentences = []
        while line:
            movie_id = line.split("::")[0]
            movie_name = line.split("::")[1].split(" (")[0].replace(")","")
            release_year = line.split("::")[1].split(" (")[1].split(")")[0]
            genre = line.split("::")[2].strip().split("|")
            print(line)
            print(movie_id,movie_name,release_year,genre)
            if int(release_year) >= 2010:
                file_ptr.write("%s\t%s\t%s\t" % (movie_id,movie_name,release_year))
                file_ptr.flush()
                for ind_genre in genre:
                    file_ptr.write("%s," %(ind_genre))
                    file_ptr.flush()
                file_ptr.write("\n")
                file_ptr.flush()
            line = f.readline()
    file_ptr.close()

if __name__ == '__main__':
    main()
