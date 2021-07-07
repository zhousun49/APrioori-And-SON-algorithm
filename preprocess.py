import pyspark 
from pyspark import SparkContext
import csv

sc = SparkContext()
sc.setLogLevel("OFF")
sc.setSystemProperty('spark.driver.memory', '4g')
sc.setSystemProperty('spark.executor.memory', '4g')
businessRDD = sc.textFile('./business.json')
reviewRDD = sc.textFile('./review.json')

def getBusiness(row): 
    elements = row.split('","')
    # print(elements)
    returned_ele = []
    for j in elements:
        if "business_id" in j:
            # print("single ele: ", j)
            sep = j.split(":")
            returned_ele.append(sep[1].strip('"'))
    for j in elements:
        if 'user_id' in j:
            sep = j.split(":")
            returned_ele.append(sep[1].strip('"'))
    return tuple(returned_ele)

def getCategory(row): 
    elements = row.split(',"')
    returned_ele = []
    business_id = ""
    for j in elements:
        # print("1, ", j)
        if "business_id" in j:
            sep = j.split('":"')
            business_id = sep[1].strip('"')
            returned_ele.append(business_id)
        if 'state":' in j:
            sep = j.split('":')
            # print(sep[1] == "null")
            if sep[1] != "null":
                cats = sep[1].strip('"')
                returned_ele.append(cats.strip())
    return tuple(returned_ele)

review_mapping = reviewRDD.map(getBusiness)
business_mapping = businessRDD.map(getCategory).filter(lambda x: x[1] == "NV")
# print("Review Mapping Count: ", review_mapping.take(2))
# print("Business Mapping Count: ", business_mapping.take(100))
# print(business_mapping.take(5))
f = open("all_reviews.csv", "a")
for i in review_mapping.collect():
    lst = list(i)
    string = ""
    for l in lst: 
        string += l
        string += ","
    string = string.strip(",")
    f.write(string)
    f.write('\n')

# # ##Union
combinedRDD = business_mapping.join(review_mapping).map(lambda x: (x[1][1], x[0]))
# print(combinedRDD.take(5))
collect = combinedRDD.collect()

f = open("ub1.csv", "a")
for i in collect:
    lst = list(i)
    string = ""
    for l in lst: 
        string += l
        string += ","
    string = string.strip(",")
    f.write(string)
    f.write('\n')


