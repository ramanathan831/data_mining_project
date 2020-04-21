import sys
import csv
import urllib.request
import re

def crewdata_dump_to_tsv(crew_data_filename, general_meta_data_filename):
    inpfileptr1 = open(crew_data_filename,"r")
    line1 = inpfileptr1.readline()

    inpfileptr2 = open("structured_" + (general_meta_data_filename.replace(".txt",".tsv")),"r")
    line2 = inpfileptr2.readline()

    outfileptr = open("final_structured_" + (general_meta_data_filename.replace(".txt",".tsv")),"w")
    outfileptr.write(line2.strip()+"\tDirector\tCast\tScreenplay\tDOP\tEditor\tMusic_Composer\tAssistant_Director\n")
    while line1:
        line2 = inpfileptr2.readline()
        id_from_file_2 = line2.split("\t")[0]
        id_from_file_1 = line1.partition('{"id":')[2].split(",")[0]

        if(id_from_file_1 != id_from_file_2):
            exit(1)
        cast = line1.partition('"cast":[')[2].split('],"crew"')[0]
        cast_data = ""
        regex = r"\{(.*?)\}"
        matches = re.finditer(regex, cast, re.MULTILINE | re.DOTALL)
        for matchNum, match in enumerate(matches):
            for groupNum in range(0, len(match.groups())):
                cast_data += (match.group(1).split('"name":"')[1].split('",')[0] + ",")

        crew = line1.partition('"crew":[')[2].split("\n")[0]

        director = ""
        screenplay = ""
        dop = ""
        editor = ""
        music_composer= ""
        assistant_director = ""

        regex = r"\{(.*?)\}"
        matches = re.finditer(regex, crew, re.MULTILINE | re.DOTALL)
        for matchNum, match in enumerate(matches):
            for groupNum in range(0, len(match.groups())):
                crew_name = match.group(1).split('"name":"')[1].split('",')[0]
                crew_job = match.group(1).split('"job":"')[1].split('",')[0]
                if (crew_job== "Director"):
                    director += (crew_name + ",")
                if (crew_job== "Screenplay"):
                    screenplay += (crew_name + ",")
                if (crew_job== "Director of Photography"):
                    dop += (crew_name + ",")
                if (crew_job== "Editor"):
                    editor += (crew_name + ",")
                if (crew_job== "Original Music Composer"):
                    music_composer += (crew_name + ",")
                if (crew_job== "Assistant Director"):
                    assistant_director += (crew_name + ",")

        outfileptr.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" %(line2.replace("\n",""),director,cast_data,screenplay,dop,editor,music_composer,assistant_director))
        # exit(1)

        line1 = inpfileptr1.readline()

def general_metadatadump_to_tsv(filename):
    inpfileptr = open(filename,"r")
    line = inpfileptr.readline()
    outfileptr = open("structured_" + (filename.replace(".txt",".tsv")),"w")
    outfileptr.write("TMDB_ID\tAdult\tBudget\tGenre_ID\tGenre_Name\tIMDB_ID\tMovie_Name\tLanguage\tSynopsis\tPopularity\tProduction_Company_IDs\tProduction_Company_Names\tProduction_Country_Names\tRelease_Date\tRunning_Time\tVote_Count\tVote_Average\tRevenue\tTagline\n")
    while line:
        response = line
        response = response[1:len(response)-2]
        adult = response.partition('"adult":')[2].split(",")[0]
        budget = response.partition('"budget":')[2].split(",")[0]
        tmdb_id = response.partition('","id":')[2].split(",")[0]
        if(tmdb_id == ""):
            tmdb_id = response.partition('null,"id":')[2].split(",")[0]
        genres = response.partition('"genres":')[2].split("]")[0]
        genres = genres[2:len(genres)-1].replace("{","").replace("}","")
        number_of_genres = len(genres.split('"id":'))
        genre_id = ""
        genre_name = ""
        for index in range(1,number_of_genres):
            genre_id += (genres.split('"id":')[index].split(",")[0]) + ","
            genre_name += (genres.split('"name":')[index].split(",")[0].replace('"','')) + ","

        imdb_id = response.partition('"imdb_id":"')[2].split('"')[0]
        movie_name = response.partition('"original_title":"')[2].split('"')[0]
        language = response.partition('"original_language":"')[2].split('"')[0]
        synopsis = response.partition('"overview":"')[2].split('"')[0]
        popularity = response.partition('"popularity":')[2].split(",")[0]

        production_companies = response.partition('"production_companies":')[2].split("],")[0]
        production_companies = production_companies[2:len(production_companies)-1].replace("{","").replace("}","")
        number_of_production_companies = len(production_companies.split('"id":'))
        production_company_id = ""
        production_company_name = ""
        production_country_name = ""
        for index in range(1,number_of_production_companies):
            if (len(production_companies.split('"id":')[index].split(",")) > 0):
                production_company_id += (production_companies.split('"id":')[index].split(",")[0]) + ","
            if (len(production_companies.split('"name":')[index].split(",")) > 0):
                production_company_name += (production_companies.split('"name":')[index].split(",")[0].replace('"','')) + ","
            if (len(production_companies.split('"origin_country":')[index].split(",")) > 0):
                production_country_name += (production_companies.split('"origin_country":')[index].split(",")[0].replace('"','')) + ","

        release_date = response.partition('"release_date":"')[2].split('"')[0]
        running_time = response.partition('"runtime":')[2].split(",")[0]
        vote_count = response.partition('"vote_count":')[2].split(",")[0]
        vote_average = response.partition('"vote_average":')[2].split(",")[0]
        revenue = response.partition('"revenue":')[2].split(",")[0]
        tagline = response.partition('"tagline":"')[2].split('","title"')[0]

        outfileptr.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"
                        %(tmdb_id, adult,budget,genre_id,genre_name,imdb_id,movie_name,language,synopsis,popularity,production_company_id,
                            production_company_name,production_country_name,release_date,running_time,vote_count,vote_average,revenue,tagline))
        outfileptr.flush()
        line = inpfileptr.readline()

def main():
    url = "https://api.themoviedb.org/3/movie/"
    api_key = "?api_key=66c98a5c3b1c83ffaf23f1c8a5326606"
    results_path=r"movie_meta_information.csv"

    mode = sys.argv[1]
    general_meta_data_filename = sys.argv[2]
    crew_data_filename = sys.argv[3]
    data_type_fetching = sys.argv[4]

    if (mode == "api_call"):
        file = open(filename,"w")
        for i in range(0,100000):
            print(i)
            if(data_type_fetching == "movie_metadata"):
                final_url = url + str(i) + api_key
            elif(data_type_fetching == "crew_data"):
                final_url = url + str(i) + "/credits" + api_key
            try:
                with urllib.request.urlopen(final_url) as grabbed_url:
                    response = grabbed_url.read().decode("utf-8")
                    file.write(response)
                    file.write("\n")
                    file.flush()
            except:
                continue
    elif (mode == "clean_metadata"):
        if(data_type_fetching == "movie_metadata"):
            general_metadatadump_to_tsv(general_meta_data_filename)
        elif(data_type_fetching == "crew_data"):
            crewdata_dump_to_tsv(crew_data_filename, general_meta_data_filename)
if __name__ == '__main__':
    main()
