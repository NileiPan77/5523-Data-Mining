import pyspark
import argparse
import re
from pyspark.sql import SparkSession
import json
import os

def stop_filter(x,filter_list):
	for i in range(len(x)):
		x[i] = re.sub('[\[\]\(\)\.\?!,;:]','',x[i])
	#print(x[i])
	result = []
	for word in x:
		lower_word = word.lower()
		if lower_word != '' and lower_word not in filter_list :
			result.append((lower_word,1))
	return result

def remove_stopwords(x):
	return stop_filter(re.split('\s',x),stopwords)
class task1:
	def __init__(self, rdd, arg):
		self.answer = "{"
		self.args = arg
		self.rdd = rdd

	def review_counts(self):
		counts = self.rdd.map(lambda x: 1).reduce(lambda a,b: a+b)
		self.answer = self.answer + "\"A\": " + str(counts) + ", "

	def review_years(self):
		year = self.args.y
		counts = self.rdd.map(lambda x: 1 if int(x.date[:4]) == year else 0).reduce(lambda a,b: a+b)
		self.answer = self.answer + "\"B\": " + str(counts) + ", "

	def distinct_user(self):
		counts = self.rdd.map(lambda x: (x.user_id,1)).reduceByKey(lambda a,b: 1).collect()
		self.answer = self.answer + "\"C\": " + str(len(counts)) + ", "

	def top_m_user(self):
		counts = self.rdd.map(lambda x: (x.user_id,1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: -x[1]).take(self.args.m)
		count_list = [list(x) for x in counts]
		count_list = json.dumps(count_list)
		self.answer = self.answer + "\"D\": " + str(count_list) + ", "
	
	def top_n_words(self):
		counts = self.rdd.flatMap(lambda x: remove_stopwords(x.text)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: -x[1]).take(self.args.n)
		count_list = [x[0] for x in counts]
		count_list = json.dumps(count_list)
		self.answer = self.answer + "\"E\": " + str(count_list) + "}"

	def solve(self):
		self.review_counts()
		self.review_years()
		self.distinct_user()
		self.top_m_user()
		self.top_n_words()
		f = open(self.args.output_file,"w")
		f.write(self.answer)
		f.close()

def arg_parse():
	parser = argparse.ArgumentParser(description='A1T1')
	parser.add_argument('--input_file',type=str, default='./data/review.json', help='the input file ')
	parser.add_argument('--output_file', type=str, default='./data/a1t1.json', help='the output file contains your answers')
	parser.add_argument('--stopwords', type=str,default='./data/stopwords',help='the file contains the stopwords')
	parser.add_argument('--y', type=int,default=2018,help='year')
	parser.add_argument('--m', type=int,default=10,help='top m users')
	parser.add_argument('--n', type=int,default=10,help='top n frequent words')
	return parser.parse_args()

if __name__ == "__main__":
	
	args = arg_parse()

	sc_conf = pyspark.SparkConf() \
		.setAppName('task1') \
		.setMaster('local[*]') \
		.set('spark.driver.memory','8g') \
		.set('spark.executor.memory','4g')
	
	sc = pyspark.SparkContext(conf=sc_conf)
	stopwords = sc.textFile(args.stopwords).collect()

	ss = SparkSession.builder \
		.master('local[*]') \
		.appName("task1") \
		.getOrCreate()

	review_data = ss.read.json(args.input_file)
	review_rdd = review_data.rdd

	task = task1(review_rdd,args)
	task.solve()
	

	
	
	
