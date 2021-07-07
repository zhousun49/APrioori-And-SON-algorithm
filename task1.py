import pyspark 
from pyspark import SparkContext
from operator import add
from itertools import combinations
import sys
import time
import math
start_time = time.time()

inputs = sys.argv
case_num = int(inputs[1])
support =  int(inputs[2])
input_file = inputs[3]
output_file = inputs[4]

##Create SparkContext and Read File
sc = SparkContext()
sc.setLogLevel("OFF")
sc.setSystemProperty('spark.driver.memory', '4g')
sc.setSystemProperty('spark.executor.memory', '4g')
inputRDD = sc.textFile(input_file)

########Answer for Task 1 Case 1
##Mapping and filter out header 
def userMap(row):
    spt = row.split(",")
    if "user" in spt[0]:
        return " "
    else: 
        return (spt[0], spt[1])
def businessMap(row):
    spt = row.split(",")
    if "user" in spt[0]:
        return " "
    else: 
        return (spt[1], spt[0])

if case_num == 1: 
    user_map = inputRDD.map(userMap)
    user_map = user_map.filter(lambda x: x != " ")
elif case_num == 2: 
    business_map = inputRDD.map(businessMap)
    business_map = business_map.filter(lambda x: x != " ")

##Group by User ID and business ID
def toList(a):
    return [a]
def add1(a,b):
    a.append(b)
    return a
def app(a, b):
    a.extend(b)
    return a
if case_num == 1: 
    combined_user_map = user_map.combineByKey(toList, add1, app).map(lambda x: x[1])
    file_list = combined_user_map.collect()
    unique_length = len(combined_user_map.collect())
elif case_num == 2: 
    combined_business_map = business_map.combineByKey(toList, add1, app).map(lambda x: x[1])
    file_list = combined_business_map.collect()
    unique_length = len(combined_business_map.collect())

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


if case_num == 1: 
    mapping_task = combined_user_map
elif case_num == 2:
    mapping_task = combined_business_map
aprior = mapping_task.mapPartitions(lambda dataset: APriorFirst(dataset, support, unique_length))
aprior = aprior.flatMap(lambda x: x)
aprior = aprior.distinct()
aprior = aprior.sortBy(lambda x: (len(x), x)).collect()
all_results = aprior
print("length: ", len(aprior))
# ##Before appending, sort and organize results
print("Support: ", support)

candidates = all_results
##Second MapReduce Task for filtering 
result = []
for i in all_results:
    if i in result:
        continue 
    else: 
        count = 0
        for line in file_list:
            if set(i).issubset(set(line)):
                count += 1
        # print("item: ", i , " has count of ",count )
        if count >= support: 
            result.append(i)
all_results = result   
# print("result: ", all_results)
    
##Element Operation for candidates
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
    for i in all_results:
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