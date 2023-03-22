import argparse
import json
import time
import pyspark
from collections import defaultdict
from itertools import combinations
import random
import math


# probablistic primality test
def is_prime_miller_rabin(n, num_tests):
    # Find power_of_two and odd_multiple such that n = 2^power_of_two * odd_multiple + 1
    odd_multiple = n - 1
    power_of_two = 0
    while odd_multiple % 2 == 0:
        odd_multiple //= 2
        power_of_two += 1

    # Perform the Miller-Rabin test num_tests times
    for _ in range(num_tests):
        random_base = random.randint(2, n - 2)
        mod_exp_result = pow(random_base, odd_multiple, n)

        if mod_exp_result == 1 or mod_exp_result == n - 1:
            continue

        for _ in range(power_of_two - 1):
            mod_exp_result = pow(mod_exp_result, 2, n)
            if mod_exp_result == n - 1:
                break
        else:
            return False

    return True

def nearest_prime_greater_than_n(n, num_tests=5):
    candidate = n + 1 if n % 2 == 0 else n + 2
    while not is_prime_miller_rabin(candidate, num_tests):
        candidate += 2
    return candidate

def make_hashes(m):
    a = random.sample(range(1,m), num_hashes)
    b = random.sample(range(1,m), num_hashes)

    p = [nearest_prime_greater_than_n(m)]
    for i in range(num_hashes):
        p.append(nearest_prime_greater_than_n(p[i]))
    
    return (a,b,p)

def Hash(a,x,b,p,m):
    return (a * x + b) % p % m

def signature(user_list, hash_params, m):
    sig = [float(m+1) for i in range(num_hashes)]
    for user_id in user_list:
        for i in range(num_hashes):
            h = Hash(hash_params[0][i], user_id, hash_params[1][i], hash_params[2][i], m)
            if h < sig[i]:
                sig[i] = h
    return sig

def split_bands(business_signature_pair):
    band = []
    for i in range(num_bands):
        # ((i, band_data), business_id)
        band.append(((i, tuple(business_signature_pair[1][i * rows_per_band: (i+1) * rows_per_band])), business_signature_pair[0]))
    # print(band)    
    return band

def print_rdd(x):
    print(x)

def make_pairs(candidates):
    return list(combinations(sorted(candidates),2))


def jaccard_similarity(b1,b2):
    c1 = set(b1)
    c2 = set(b2)
    inter = len(c1.intersection(c2))
    return 1.0 if inter == len(c1) and inter == len(c2) else inter / (len(c1) + len(c2) - inter)

def main(input_file, output_file, jac_thr, n_bands, n_rows, sc):
    review_rdd = sc.textFile(input_file).map(lambda x: x.split("\n")).map(lambda x: json.loads(x[0]))
    
    # print(review_rdd.first())
    user_count = review_rdd.map(lambda x: x['user_id']).distinct().collect()
    # business_count = len(review_rdd.map(lambda x: x['business_id']).distinct().collect())
    unique_user_count = len(user_count)
    # print("unique user count: " + str(unique_user_count))
    # print("unique business count: " + str(business_count))
    user_indexMapping = defaultdict(int)
    for index,item in enumerate(user_count):
        user_indexMapping[item] = index
    characteristc_matrix = review_rdd.map(lambda x: (x['business_id'], user_indexMapping[x['user_id']])).groupByKey().mapValues(list).sortBy(lambda x: x[0]).persist()
    
    characteristc_matrix_mapping = characteristc_matrix.collectAsMap()
    hash_params = sc.broadcast(make_hashes(unique_user_count*2))
    signature_m = characteristc_matrix.map(lambda x: (x[0], signature(x[1], hash_params.value, unique_user_count))).persist()
    # signature_matrix = signature_m.collect()

    candidate_pairs =  signature_m.flatMap(lambda x: split_bands(x)) \
        .groupByKey().mapValues(set).map(lambda x: x[1]).filter(lambda x: len(x) > 1) \
        .flatMap(lambda x: make_pairs(x)).distinct()
    similar_candidates = candidate_pairs.map(lambda x: (x[0], x[1], jaccard_similarity(characteristc_matrix_mapping[x[0]], characteristc_matrix_mapping[x[1]]))) \
        .filter(lambda x: x[2] >= jac_thr)
    similar_candidates = similar_candidates.collect()

    fd = open(output_file, 'w')

    for item in similar_candidates:
        # print(item)
        fd.write("{\"b1\": \"" + str(item[0]) + "\", \"b2\": \"" + str(item[1]) + "\", \"sim\": " + str(item[2]) + "}\n")
    fd.close()
    # 
    # print(split)
    #review_rdd.foreach(print_rdd)
    """ You need to write your own code """
    #print()



if __name__ == '__main__':
    start_time = time.time()
    sc_conf = pyspark.SparkConf() \
        .setAppName('hw3_task1') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    parser = argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--input_file', type=str, default='./data/train_review.json')
    parser.add_argument('--output_file', type=str, default='./outputs/task1.out')
    parser.add_argument('--time_file', type=str, default='./outputs/task1.time')
    parser.add_argument('--threshold', type=float, default=0.1)
    parser.add_argument('--n_bands', type=int, default=50)
    parser.add_argument('--n_rows', type=int, default=2)
    args = parser.parse_args()

    
    num_bands = args.n_bands
    rows_per_band = args.n_rows
    num_hashes = num_bands * rows_per_band
    main(args.input_file, args.output_file, args.threshold, args.n_bands, args.n_rows, sc)
    sc.stop()

    # log time
    with open(args.time_file, 'w') as outfile:
        json.dump({'time': time.time() - start_time}, outfile)
    print('The run time is: ', (time.time() - start_time))


