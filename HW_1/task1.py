import pyspark
import argparse
import re
from pyspark.sql import SparkSession
import os

input_file_path = './data/review.json'

def review_counts(rdd):
	counts = rdd.map(lambda x: (1,1)).reduceByKey(lambda a,b: a+b).collect()
	print("Count of total review: "+ str(counts[0][1]))

def review_years(rdd,year):
	counts = rdd.map(lambda x: (1,1) if int(x.date[:4]) == year else (2,1)).reduceByKey(lambda a,b: a+b).collect()
	print(counts)
	print("Count of review in year " + str(year) + ": "+ str(counts[0][1]))

def distinct_user(rdd):
	counts = rdd.map(lambda x: (x.user_id,1)).reduceByKey(lambda a,b: a+b).collect()
	print("Count of total distinct reviewed users: "+ len(counts))

def top_m_user(rdd,m):
	counts = rdd.map(lambda x: (x.text,1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: -x[1]).take(m)
	print("Count of total distinct reviewed users: ")
	print(counts)


def stop_filter(x,filter_list):
	result = []
	for word in x:
		lower_word = word.lower()
		if lower_word != '' and lower_word not in filter_list :
			result.append((lower_word,1))
	return result

def remove_stopwords(x):
	return stop_filter(re.split('\s',re.sub(r'[^a-zA-Z0-9\s\t]','',x)),stopwords)
	
def top_n_words(rdd,n):
	counts = rdd.flatMap(lambda x: remove_stopwords(x.text)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: -x[1]).take(n)
	print("Count of total distinct reviewed users: ")
	print(counts[:n])

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='A1T1')
	parser.add_argument('--input_file',type=str, default='./data/review.json', help='the input file ')
	parser.add_argument('--output_file', type=str, default='./data/a1t1.json', help='the output file contains your answers')
	parser.add_argument('--stopwords', type=str,default='./data/stopwords',help='the file contains the stopwords')
	parser.add_argument('--y', type=int,default=2018,help='year')
	parser.add_argument('--m', type=int,default=10,help='top m users')
	parser.add_argument('--n', type=int,default=10,help='top n frequent words')
	args = parser.parse_args()

	sc_conf = pyspark.SparkConf() \
		.setAppName('task1') \
		.setMaster('local[*]') \
		.set('spark.driver.memory','8g') \
		.set('spark.executor.memory','4g')
	
	sc = pyspark.SparkContext(conf=sc_conf)
	sc.setLogLevel("OFF")
	stopwords = sc.textFile(args.stopwords).collect()

	ss = SparkSession.builder \
		.master('local[*]') \
		.appName("task1") \
		.getOrCreate()

	review_data = ss.read.json(args.input_file)
	# review_data.show()
	review_rdd = review_data.rdd

	#review_counts(review_rdd)
	print(args)
	top_n_words(review_rdd,args.n)
	
	

	#review_rdd = sc.parallelize(review_data)
	
	

	
	
	
