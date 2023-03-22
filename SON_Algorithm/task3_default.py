import pyspark
import argparse
import re
import json
from pyspark.sql import SparkSession
import os

input_file_path = './data/review.json'

def category_split(str):
	if str != None:
		comma_splitted = re.split(",",str)
		categories = [re.sub('[\s\t]','',x) for x in comma_splitted]
		return categories
	else:
		return []

def f(iter):
	yield sum(1 for i in iter)

class task3:
	def __init__(self, rdd, arg):
		self.answer = "{\"n_partitions\": " + str(rdd.getNumPartitions()) + ", \"n_items\": "
		self.args = arg
		self.rdd = rdd
	
	def review_n_count(self):
		threshold = self.args.n
		size_partition = self.rdd.mapPartitions(f).collect()
		self.answer = self.answer + str(size_partition) + ", \"result\":"
		count = self.rdd.map(lambda x: (x.business_id,1)).reduceByKey(lambda a,b: a+b).filter(lambda x: x[1] > threshold).collect()
		count_list = [list(x) for x in count]
		count_list = json.dumps(count_list)
		self.answer = self.answer + str(count_list) + "}"
		fd = open(self.args.output_file, "w")
		fd.write(self.answer)
		fd.close()

    
def arg_parse():
    parser = argparse.ArgumentParser(description='A1T3')
    parser.add_argument('--input_file',type=str, default='./data/review.json', help='the input file ')
    parser.add_argument('--output_file', type=str,default='./data/a1t3_default.json',help='the output contains your answers')
    parser.add_argument('--n', type=int,default=200,help='top n highest review counts')
    return parser.parse_args()

if __name__ == "__main__":
	
	args = arg_parse()

	sc_conf = pyspark.SparkConf() \
		.setAppName('task1') \
		.setMaster('local[*]') \
		.set('spark.driver.memory','8g') \
		.set('spark.executor.memory','4g')
	
	sc = pyspark.SparkContext(conf=sc_conf)
	ss = SparkSession.builder \
		.master('local[*]') \
		.appName("task3") \
		.getOrCreate()

	review_data = ss.read.json(args.input_file)

	task = task3(review_data.rdd, args)
	task.review_n_count()