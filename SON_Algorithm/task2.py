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
		categories = [x.strip() for x in comma_splitted]
		return categories
	else:
		return []
class task2:
	def __init__(self, rdd, arg):
		self.answer = "{\"result\": "
		self.args = arg
		self.rdd = rdd
	
	def average_star(self):
		cate_stars = self.rdd.flatMap(lambda x: [(cate, x.stars) for cate in category_split(x.categories)]).persist()
		key_sum_pair = cate_stars.reduceByKey(lambda a,b: a+b)
		key_count_pair = cate_stars.map(lambda x: (x[0],1)).reduceByKey(lambda a,b: a+b)
		# print(key_sum_pair.collect())
		# print("---------------------------------------------------------------------------------------------------------------")
		# print(key_count_pair.collect())
		# print("---------------------------------------------------------------------------------------------------------------")
		joined = key_sum_pair.join(key_count_pair)
		# print("---------------------------------------------------------------------------------------------------------------")
		# print(joined.collect())
		ans = joined.map(lambda x: (x[0],x[1][0]/x[1][1])).sortBy(lambda x: x[0]).sortBy(lambda x: -x[1]).take(self.args.n)
		#print(ans)
		ans_list = [list(x) for x in ans]
		ans_list = json.dumps(ans_list)
		self.answer = self.answer + str(ans_list) + "}"
		f = open(self.args.output_file,"w")
		f.write(self.answer)
		f.close()

    
def arg_parse():
    parser = argparse.ArgumentParser(description='A1T2')
    parser.add_argument('--review_file',type=str, default='./data/review.json', help='the input file ')
    parser.add_argument('--business_file', type=str, default='./data/business.json', help='the business dataset')
    parser.add_argument('--output_file', type=str,default='./data/a1t2.json',help='the output contains your answers')
    parser.add_argument('--n', type=int,default=10,help='top n highest average stars')
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
		.appName("task2") \
		.getOrCreate()

	review_data = ss.read.json(args.review_file)
	business_data = ss.read.json(args.business_file)

	joined_data = review_data.join(business_data.drop("stars"), on="business_id")

	joined_rdd = joined_data.rdd
	task = task2(joined_rdd, args)
	task.average_star()