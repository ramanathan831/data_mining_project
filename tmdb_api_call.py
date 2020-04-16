import sys
import csv
import urllib.request

def dump_to_csv(filename):
    fileptr = open(filename,"r")
    lines = fileptr.readlines()
    for response in lines:
        response = response[1:len(response)-2]
        adult = response.partition('"adult":')[2].split(",")[0]
        budget = response.partition('"budget":')[2].split(",")[0]

        print(response)
        genres = response.partition('"genres":')[2].split("]")[0]
        genres = genres[2:len(genres)-1].replace("{","").replace("}","")
        number_of_genres = len(genres.split('"id":'))
        genre_id = []
        genre_name = []
        for index in range(1,number_of_genres):
            genre_id.append(genres.split('"id":')[index].split(",")[0])
            genre_name.append(genres.split('"name":')[index].split(",")[0].replace('"',''))

        imdb_id = response.partition('"imdb_id":"')[2].split('"')[0]
        movie_name = response.partition('"original_title":"')[2].split('"')[0]
        language = response.partition('"original_language":"')[2].split('"')[0]
        synopsis = response.partition('"overview":"')[2].split('"')[0]
        popularity = response.partition('"popularity":')[2].split(",")[0]

        production_companies = response.partition('"production_companies":')[2].split("]")[0]
        print(production_companies)
        # exit(1)
        production_companies = production_companies[2:len(production_companies)-1].replace("{","").replace("}","")
        print(production_companies)
        number_of_production_companies = len(production_companies.split('"id":'))
        production_company_id = []
        production_company_name = []
        production_country_name = []
        for index in range(1,number_of_production_companies):
            production_company_id.append(production_companies.split('"id":')[index].split(",")[0])
            production_company_name.append(production_companies.split('"name":')[index].split(",")[0].replace('"',''))
            production_country_name.append(production_companies.split('"origin_country":')[index].split(",")[0].replace('"',''))

        release_date = response.partition('"release_date":"')[2].split('"')[0]
        running_time = response.partition('"runtime":')[2].split(",")[0]
        vote_count = response.partition('"vote_count":')[2].split(",")[0]
        vote_average = response.partition('"vote_average":')[2].split(",")[0]
        revenue = response.partition('"revenue":')[2].split(",")[0]
        tagline = response.partition('"tagline":"')[2].split('"')[0]
        #{"adult":false,"backdrop_path":"/bSvUW4JQ6g4QiKvwejcfcPRd4Ke.jpg","belongs_to_collection":null,"budget":150000000,"genres":[{"id":12,"name":"Adventure"},{"id":35,"name":"Comedy"},{"id":10751,"name":"Family"},{"id":14,"name":"Fantasy"},{"id":10402,"name":"Music"}],"homepage":"https://www.warnerbros.com/charlie-and-chocolate-factory","id":118,"imdb_id":"tt0367594","original_language":"en","original_title":"Charlie and the Chocolate Factory","overview":"A young boy wins a tour through the most magnificent chocolate factory in the world, led by the world's most unusual candy maker.","popularity":27.361,"poster_path":"/Oop38B7iJdykEkpAhNZKbEz91t.jpg","production_companies":[{"id":79,"logo_path":"/tpFpsqbleCzEE2p5EgvUq6ozfCA.png","name":"Village Roadshow Pictures","origin_country":"US"},{"id":80,"logo_path":null,"name":"The Zanuck Company","origin_country":"US"},{"id":8601,"logo_path":null,"name":"Tim Burton Productions","origin_country":""},{"id":55512,"logo_path":null,"name":"Theobald Film Productions","origin_country":""},{"id":174,"logo_path":"/IuAlhI9eVC9Z8UQWOIDdWRKSEJ.png","name":"Warner Bros. Pictures","origin_country":"US"},{"id":81,"logo_path":"/8wOfUhA7vwU2gbPjQy7Vv3EiF0o.png","name":"Plan B Entertainment","origin_country":"US"}],"production_countries":[{"iso_3166_1":"AU","name":"Australia"},{"iso_3166_1":"GB","name":"United Kingdom"},{"iso_3166_1":"US","name":"United States of America"}],"release_date":"2005-07-13","revenue":474968763,"runtime":115,"spoken_languages":[{"iso_639_1":"en","name":"English"}],"status":"Released","tagline":"Prepare for a taste of adventure.","title":"Charlie and the Chocolate Factory","video":false,"vote_average":7.0,"vote_count":9639}

        print("adult ",adult)
        print("budget ",budget)
        print("genre_id ", genre_id)
        print("genre_name ", genre_name)
        print("imdb_id ", imdb_id)
        print("movie_name ", movie_name)
        print("language ", language)
        print("synopsis ", synopsis)
        print("popularity ", popularity)
        print("production_company_id ",production_company_id)
        print("production_company_name ",production_company_name)
        print("production_country_name ",production_country_name)
        print("release_date ",release_date)
        print("running_time ",running_time)
        print("vote_count ",vote_count)
        print("vote_average ",vote_average)
        print("revenue ",revenue)
        print("tagline ",tagline)
        exit(1)
def main():
    url = "https://api.themoviedb.org/3/movie/"
    api_key = "?api_key=66c98a5c3b1c83ffaf23f1c8a5326606"
    results_path=r"movie_meta_information.csv"

    mode = sys.argv[1]
    filename = sys.argv[2]
    data_type_fetching = sys.argv[3]

    if (mode == "api_call"):
        file = open("dmp.txt","w")
        for i in range(0,100000):
            print(i)
            if(data_type_fetching == "movie_metadata"):
                final_url = url + str(i) + api_key
            else:
                final_url = url + str(i) + "/credits" + api_key
            try:
                with urllib.request.urlopen(final_url) as grabbed_url:
                    response = grabbed_url.read().decode("utf-8") #using decode as the data was in bytes
                    file.write(response)
                    file.write("\n")
                    file.flush()
                    # exit(1)
            except:
                continue
    elif (mode == "clean_metadata"):
        dump_to_csv(filename)

if __name__ == '__main__':
    main()
