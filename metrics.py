import sys
import functools
import operator

def remove_col(list_2d, column_num):
	return [[j for i,j in enumerate(sub) if i != column_num] for sub in list_2d]

def fill_dictionary(file, file_type, hybrid_method, sentiment_dict):
	line = file.readline()
	dictionary = {}
	while line:
		user_id = line.split("\t")[0]
		movie_id = int(line.split("\t")[1])
		rating = float(line.split("\t")[2].strip())
		if(file_type == "test"):
			if(hybrid_method == 1):
				if(movie_id in sentiment_dict.keys()):
					tup = (movie_id, (rating+sentiment_dict[movie_id])/2)
				else:
					tup = (movie_id, rating)
			else:
				if(movie_id in sentiment_dict.keys()):
					tup = (movie_id, rating, sentiment_dict[movie_id])
				else:
					tup = (movie_id, rating, rating)
		else:
			tup = (movie_id, rating)

		if user_id not in dictionary.keys():
			dictionary[user_id] = [tup]
		else:
			dictionary[user_id].append(tup)
		line = file.readline()

	for key in dictionary.keys():
		value_list = dictionary[key]
		if(file_type == "test" and hybrid_method == 2):
			value_list = sorted(value_list, key=lambda x: x[2], reverse=True)
		else:
			value_list = sorted(value_list, key=lambda x: x[1], reverse=True)
		dictionary[key] = value_list

	return dictionary

def fill_sentiment_dict(file):
	line = file.readline()
	dictionary = {}
	while line:
		movie_id = int(line.split("\t")[0])
		sentiment_score = float(line.split("\t")[1].strip())
		dictionary[movie_id] = sentiment_score
		line = file.readline()
	return dictionary

def generate_hybrid_dict(prediction_values1, prediction_values2, sentiment_dict, rmse_1, rmse_2):
	dictionary = {}
	for key in prediction_values1.keys():
		if(key not in prediction_values2.keys()):
			continue
		rating_list_1 = prediction_values1[key]
		rating_list_2 = prediction_values2[key]

		lst = []
		dictionary = {}
		for element in rating_list_1:
			element = (element[0],element[1],(1/rmse_1)*element[2])
		lst = []
		for element in rating_list_2:
			element = (element[0],element[1],(1/rmse_2)*element[2])

def metric(ground_truth_values, prediction_values, sentiment_dict, hybrid_method):
	precision = 0
	recall = 0
	f1_score = 0
	for key in prediction_values.keys():
		ground_truth_list = set(ground_truth_values[key][:5])
		ground_truth_list = remove_col(ground_truth_list,1)
		ground_truth_list = [list(elem) for elem in ground_truth_list]
		ground_truth_list = set(functools.reduce(operator.iconcat, ground_truth_list, []))

		prediction_list = set(prediction_values[key][:5])
		if(hybrid_method == 1):
			prediction_list = remove_col(prediction_list,1)
		else:
			prediction_list = remove_col(prediction_list,1)
			prediction_list = remove_col(prediction_list,1)
		prediction_list= [list(elem) for elem in prediction_list]
		prediction_list = set(functools.reduce(operator.iconcat, prediction_list, []))

		tp_elements = prediction_list - (prediction_list - ground_truth_list)
		fp_elements = prediction_list - ground_truth_list
		fn_elements = ground_truth_list - prediction_list
		tp_count = len(tp_elements)
		fp_count = len(fp_elements)
		fn_count = len(fn_elements)

		individual_precision = tp_count / float(tp_count+fp_count)
		individual_recall = tp_count / float(tp_count+fn_count)
		if(individual_recall == 0 and individual_precision == 0):
			individual_f1 = 0
		else:
			individual_f1 = 2*individual_precision*individual_recall / float(individual_recall + individual_precision)

		precision += individual_precision
		recall += individual_recall
		f1_score += individual_f1

	print(precision,len(prediction_values.keys()))
	print(recall,len(prediction_values.keys()))
	print(f1_score,len(prediction_values.keys()))

	print(precision/len(prediction_values.keys()))
	print(recall/len(prediction_values.keys()))
	print(f1_score/len(prediction_values.keys()))

def main():
	movies_file = open(sys.argv[1],"r") #rating_ground_truth file
	sentiment_file = open(sys.argv[2],"r") #sentiment analaysis file
	sentiment_combining_method = int(sys.argv[3]) #0 for just content-based or collobrative, 1 for average of sentiment score and recommendation technique, 2 for sorting movies based on sentiment
	pred_file = open(sys.argv[4],"r") #prediction file1
	total_hybrid = int(sys.argv[5]) #1 to merge all 3 methods else 0
	if(total_hybrid):
		pred_file2 = open(sys.argv[6],"r") #prediction file2 needed for total hybrid

	sentiment_dict = fill_sentiment_dict(sentiment_file)
	ground_truth_values = fill_dictionary(movies_file, "ground_truth", sentiment_combining_method, sentiment_dict)

	if(total_hybrid):
		prediction_values1 = fill_dictionary(pred_file, "test", sentiment_combining_method, sentiment_dict)
		prediction_values2 = fill_dictionary(pred_file2, "test", sentiment_combining_method, sentiment_dict)
		content_based_rmse = 2.4
		colloborative_rmse = 1.5
		prediction_values = generate_hybrid_dict(prediction_values1, prediction_values2, sentiment_dict, content_based_rmse, colloborative_rmse)
		precision_val = metric(ground_truth_values, prediction_values1, sentiment_dict, sentiment_combining_method)

	else:
		prediction_values1 = fill_dictionary(pred_file, "test", sentiment_combining_method, sentiment_dict)
		precision_val = metric(ground_truth_values, prediction_values1, sentiment_dict, sentiment_combining_method)

	movies_file.close()
	sentiment_file.close()
	pred_file.close()

if __name__ == '__main__':
	main()
