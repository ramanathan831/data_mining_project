#python train_test_split.py datasets/ratings_cleaned.txt
import sys
import random
import ntpath

def main():
	inp_file = open(sys.argv[1],"r")
	train_file_name = sys.argv[1].replace(ntpath.basename(sys.argv[1]),"train_set_1.txt")
	test_file_name =  sys.argv[1].replace(ntpath.basename(sys.argv[1]),"test_set_1.txt")
	train_file = open(train_file_name,"w")
	test_file = open(test_file_name,"w")

	dictionary = {}
	line = inp_file.readline()
	counter = 0
	while(line):
		movie_id = line.split("\t")[0]
		user_id = line.split("\t")[1]
		rating = line.split("\t")[2]
		if not(user_id in dictionary):
			dictionary[user_id] = [[movie_id,rating]]
		else:
			dictionary[user_id].append([movie_id,rating])
		line = inp_file.readline()

	for user_id in dictionary:
		if(len(dictionary[user_id]) >=5):
			ratings = dictionary[user_id]
			random.shuffle(ratings)
			num_training_samples = int(0.8*len(ratings))

			train_list = ratings[:num_training_samples]
			test_list = ratings[num_training_samples:]

			for individual_rating in train_list:
				train_file.write("%s\t%s\t%s\n" %(user_id, individual_rating[0], individual_rating[1]))
				train_file.flush()
			for individual_rating in test_list:
				test_file.write("%s\t%s\t%s\n" %(user_id, individual_rating[0], individual_rating[1]))
				test_file.flush()

if __name__ == '__main__':
	main()
