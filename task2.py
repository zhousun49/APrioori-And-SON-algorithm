import pyspark 
from pyspark import SparkContext, StorageLevel, SparkConf
from operator import add
from itertools import combinations
from itertools import groupby
import sys
import time
import math 

start_time = time.time()

inputs = sys.argv
filter_threshold = int(inputs[1])
support =  int(inputs[2])
input_file = inputs[3]
output_file = inputs[4]

##Create SparkContext and Read File
configuration = SparkConf()
configuration.set("spark.driver.memory", "4g")
configuration.set("spark.executor.memory", "4g")
sc = SparkContext.getOrCreate(configuration)
inputRDD = sc.textFile(input_file)

def userMap(row):
    spt = row.split(",")
    return (spt[0], spt[1])
header = inputRDD.first()
user_map = inputRDD.filter(lambda x: x != header).map(userMap)

##Group by User ID
combined_user_map = user_map.groupByKey().map(lambda x: list(set(x[1])))
combined_user_map = combined_user_map.filter(lambda x: len(x) > filter_threshold)
file_list = combined_user_map.collect()
length = len(file_list)
print("unique users that fits condition: ", length)

##Check if the result satisfies the support 
def checkReal(dictionary, real_support):
    candidates = []
    for k, v in dictionary.items():
        if v >= real_support:
            candidates.append(k)
    return sorted(candidates)

# Multiple Combinations
def multipleCandidates(partition, candidates, size): 
    possible_multiple_dict = {}
    for lst in partition: 
        ##Only Overlapping items
        no_repeat_options = sorted(set(lst) & set(candidates))
        combine = combinations(no_repeat_options, size)
        for i in combine:
            i = tuple(i)
            if i in possible_multiple_dict:
                possible_multiple_dict[i] += 1
            else:
                possible_multiple_dict[i] = 1
    return possible_multiple_dict

def APriorFirst(basket, support, length):
    partition = list(basket)
    freqItems = []
    singles = []
    possible_1s_dict = {}
    for lst in partition: 
        for sing in lst:
            if sing in possible_1s_dict:
                possible_1s_dict[sing] += 1
            else:
                possible_1s_dict[sing] = 1
    potential_ones = possible_1s_dict
    real_support = math.ceil(len(partition)/length*support)
    print("support: ", real_support)
    candidates = checkReal(potential_ones, real_support)
    ##Conversion of single items to tuples
    for i in candidates:
        singles.append((i,))
    freqItems.append(singles)
    print("nummber singles: ", len(freqItems[0]))
    # print("Ones candidates done")

    size = 2
    while True: 
        potential_multiple = multipleCandidates(partition, candidates, size)
        result = checkReal(potential_multiple, real_support)
        if len(result) == 0:
            break
        # print("first length: ", len(result[0]))
        # print("last length: ", len(result[-1]))
        freqItems.append(result)
        ##Append results to new candidates
        candidates = set()
        for i in result:
            candidates = candidates | set(i)
        # candidates = set() | set(result)
        print("Done Size in : ", size)
        size += 1
    # print("doubles and more: ", freqItems)
    return freqItems

##Phase 1
aprior = combined_user_map.mapPartitions(lambda dataset: APriorFirst(dataset, support, length))
aprior = aprior.flatMap(lambda x: x)
aprior = aprior.distinct().collect()
# print("final", aprior[0:5])
print("length: ", len(aprior))
# ##Before appending, sort and organize results
print("Support: ", support)

all_results = sorted(aprior, key = lambda x: x[0])
all_results = sorted(all_results, key=lambda x: len(x))
candidates = all_results
##Second MapReduce Task for filtering 
result = []
for i in all_results:
    if i in result:
        continue 
    else: 
        count = 0
        for line in file_list:
            # print("one line: ", line)
            if set(i).issubset(set(line)):
                count += 1
        # print("item: ", i , " has count of ",count )
        if count >= support: 
            result.append(i)  
# print("result: ", all_results)
    
# ##Element Operation for candidates
item_length = 1
longest_candid = len(candidates[-1])
print("Longest candidate: ", longest_candid)

##First element needs some string operation
cand = []
while item_length <= longest_candid: 
    single_size = []
    current_length = 0
    for i in candidates:
        if len(i) == item_length and item_length == 1:
            single_size.append("('" + "".join(i)+"')")
        elif len(i) == item_length:
            single_size.append(str(i))
    item_length += 1
    cand.append(",".join(single_size))

##Element Operation
item_length = 1
longest_freq = len(all_results[-1])
print("Longest frequent element: ", longest_freq)

##First element needs some string operation
lines = []
while item_length <= longest_freq: 
    single_size = []
    current_length = 0
    for i in result:
        if len(i) == item_length and item_length == 1:
            single_size.append("('" + "".join(i)+"')")
        elif len(i) == item_length:
            single_size.append(str(i))
    item_length += 1
    lines.append(",".join(single_size))

##Write to file
save = open(output_file, "a")
save.write("Candidates: \n")
for i in cand: 
    save.write(i)
    save.write("\n")
    save.write("\n")

save.write("Frequent Itemsets: \n")
for i in lines:
    save.write(i)
    save.write("\n")
    save.write("\n")

save.close()

print("Duration: ", "{:.1f}".format(time.time() - start_time), " s")