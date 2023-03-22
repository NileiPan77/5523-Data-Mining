import pyspark
import argparse
import re
from collections import defaultdict
from itertools import combinations
import numpy as np
import json
import os
import time

def arg_parse():
	parser = argparse.ArgumentParser(description='A2T1')
	parser.add_argument('--input_file',type=str, default='./data/user_business.csv', help='the input file ')
	parser.add_argument('--output_file', type=str, default='./data/a2t2.json', help='the output file contains your answers')
	parser.add_argument('--k', type=int,default=10,help='filter')
	parser.add_argument('--s', type=int,default=10,help='support threshold')
	return parser.parse_args()

def subset(left, right):
	for item in left:
		if item not in right:
			return False
	return True
def make_pair(frequent_single, dataset, support):
	candidate_count = defaultdict(int)
	for data in dataset:
		data_frequent = [i for i in data if i in frequent_single]
		candidate_chunck = combinations(data_frequent,2)
		for cand in candidate_chunck:
			cand = tuple(sorted(cand))
			candidate_count[cand] += 1
	frequent_cand = []
	for cand, count in candidate_count.items():
		if count >= support:
			frequent_cand.append(set(cand))
	return frequent_cand

def make_tuple(previous_candidate, dataset, support, size):
	candidates = []
	for x in previous_candidate:
		for y in previous_candidate:
			comb = tuple(sorted(x.union(y)))
			if len(comb) == size and comb not in candidates:
				valid = True
				for lower_comb in combinations(comb, size-1):
					if set(lower_comb) not in previous_candidate:
						valid = False
						break
				if valid:
					candidates.append(comb)
	candidate_count = defaultdict(int)
	for cand in candidates:
		for data in dataset:
			if subset(cand,data):
				candidate_count[cand] += 1
				if candidate_count.get(cand) >= support:
					break
	frequent_cand = []
	for cand, count in candidate_count.items():
		if count >= support:
			frequent_cand.append(set(cand))
	return frequent_cand


def apriori(datachunck):
	dataset = []
	chunck = list(datachunck)
	counter = defaultdict(int)
	support = args.s * len(chunck)/totalLength
	# print("chunck length: " + str(len(chunck)))
	# print("support: " + str(support))
	for data in chunck:
		dataset.append(data)
		for ele in data:
			counter[ele] += 1
	
	
	frequent_single = []
	for key,val in counter.items():
		if val >= support:
			frequent_single.append(key)
	frequent_single.sort()
	frequent_comb = []
	frequent_comb.append(frequent_single)
	frequent_comb.append([])
	single_length = len(frequent_single)
	previous_size = 1
	size = 2
	# frequent pair set
	start_time = time.time()
	frequent_pair = make_pair(frequent_single, dataset, support)
	# print("size " + str(size) + " has length: " + str(len(frequent_pair)) + ", takes time :" + str(time.time() - start_time))
	frequent_comb[previous_size] = frequent_pair
	
	while size <= single_length and len(frequent_comb[size-1]) > 0:
		size += 1
		start_time = time.time()
		frequent_tuple = make_tuple(frequent_comb[previous_size],dataset,support,size)
		# print("size " + str(size) + " has length: " + str(len(frequent_tuple)) + ", takes time :" + str(time.time() - start_time))

		previous_size += 1
		frequent_comb.append([])
		frequent_comb[previous_size] = frequent_tuple
	
	result = [[]]
	for i in frequent_comb[0]:
		result[-1].append([i])
	
	for i in range(1, len(frequent_comb)):
		result.append([])
		for cand in frequent_comb[i]:
			result[-1].append(sorted(cand))
	
	return result

def find_cand(partitionData):
	candidates = apriori(partitionData)

	for cand in candidates:
		for c in cand:
			yield (tuple(c),1)

def count_cand(Data):
	counter = defaultdict(int)
	chunck = list(Data)
	for cand in actual_test:
		for c in cand:
			for d in chunck:
				if subset(c,d):
					counter[c] += 1
	for key,value in counter.items():
		yield (key,value)

def readCase1():
	file = open(args.input_file)
	line = file.readline()
	csv = []
	read_counter = defaultdict(int)
	while True:
		line = file.readline()
		if not line:
			break
		
		data = line.rstrip().split(",")
		read_counter[data[1]] += 1
		csv.append(data)
	return csv,read_counter

def drop_non_frequent(read_counter):
	frequent_list = []
	for key in read_counter:
		if read_counter.get(key) >= args.s:
			frequent_list.append(key)
	del read_counter
	return frequent_list

if __name__ == "__main__":
	start = time.time()

	args = arg_parse()

	partitions = 2
	
	sc_conf = pyspark.SparkConf() \
		.setAppName('task1') \
		.setMaster('local[*]') \
		.set('spark.driver.memory','8g') \
		.set('spark.executor.memory','4g')
	
	sc = pyspark.SparkContext(conf=sc_conf)

	fd = open(args.output_file, "w")
	answer = "{\"Candidates\":"
	
	csv_rdd = sc.textFile(args.input_file).map(lambda x: x.split(","))

	# csv,read_counter = readCase1()
	# csv_rdd = sc.parallelize(csv)
	# frequent_list = drop_non_frequent(read_counter)

	pair_length = 2
	header = csv_rdd.first()
	
	# print("1")
	# print(time.time() - start)

	grouped_csv = csv_rdd.filter(lambda x: x != header).groupByKey().mapValues(set).filter(lambda x: len(x[1]) > args.k).map(lambda x: x[1]).persist()
	
	#parallize_Data = sc.parallelize(grouped_csv.collect())
	totalLength = len(grouped_csv.collect())
	# print("total length: " + str(totalLength))
	partitions = grouped_csv.getNumPartitions()
	# print("partitions: " + str(partitions))
	mid_result = grouped_csv.mapPartitions(find_cand).reduceByKey(lambda a,b: a+b).persist()
	candidates_mid = mid_result.collect()

	# print("2")
	# print(time.time() - start)
	#print(candidates_mid)
	mid_output = []
	result = []
	actual_test = []
	for i in range(pair_length):
		mid_output.append([])
		result.append([])
		actual_test.append([])
	
	for i,c in enumerate(candidates_mid):
		size = len(candidates_mid[i][0])-1
		if len(mid_output) <= size:
			mid_output.append([])
			result.append([])
			actual_test.append([])
		mid_output[size].append(candidates_mid[i][0])
		if candidates_mid[i][1] == partitions:
			result[size].append(candidates_mid[i][0])
		else:
			actual_test[size].append(candidates_mid[i][0])
	for mid in mid_output:
		mid.sort()
	if len(mid_output[-1]) == 0:
		mid_output = mid_output[:-1]
	
	answer += json.dumps(mid_output)
	answer += ", \"Frequent Itemsets\":"
	for i in mid_output:
		i.sort()
	passed_candidate = grouped_csv.mapPartitions(count_cand).reduceByKey(lambda a,b: a+b).filter(lambda x: x[1] >= args.s).collect()
	
	for cand in passed_candidate:
		size = len(cand[0])
		result[size-1].append(cand[0])
	for res in result:
		res.sort()
	result = [x for x in result if len(x) != 0]
	answer += json.dumps(result)
	answer += ", \"Runtime\":"
	answer += str(time.time() - start)
	answer += "}"
	fd.write(answer)
	fd.close()