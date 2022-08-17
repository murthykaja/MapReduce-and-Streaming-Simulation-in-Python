import random
from statistics import median
import numpy as np
import math
import hashlib
import csv
import pprint
import sys
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("appName")
sc = SparkContext(conf=conf)
# print(sys.argv[1])
# exit(0)
income_rdd=sc.textFile(sys.argv[1], 32)
def power10(num):
    return (int(math.pow(10, math.floor(math.log10(int(num))))),1)
power_10_rdd=income_rdd.map(lambda x: power10(x)).reduceByKey(lambda a,b: a+b)
power_10_list = power_10_rdd.collect()
key_value_rdd = income_rdd.map(lambda x: (int(x),1))
frequency_rdd = key_value_rdd.reduceByKey(lambda a,b:a+b)
mode = frequency_rdd.max(key=lambda a:a[1])
# print(mode)
count_distinct = frequency_rdd.count()
# print(count_distinct)
key_value_sorted_rdd = key_value_rdd.sortByKey()
count_total = key_value_rdd.count()
if count_total%2 == 0:
    median_pos = int(count_total/2)
    val1 = key_value_sorted_rdd.take(median_pos)[median_pos-1]
    val2 = key_value_sorted_rdd.take(median_pos+1)[median_pos]
    median_value = (val1[0]+val2[0])/2
    # print(median_value)
else:
    median_pos = count_total//2 +1
    median_value = key_value_sorted_rdd.take(median_pos)[median_pos-1]
    # print(median_value)
print("Task 1 A: Calculate distinct incomes, median of incomes, most frequent income, and count per 10power")
print("*****************************************\ncount of distinct incomes:\n"+str(count_distinct))
print("*****************************************")
print("*****************************************\nmedian of incomes:\n"+str(median_value))
print("*****************************************")
print("*****************************************\nmost frequent income:\n"+str(mode[0]))
print("*****************************************")
print("*****************************************\ncount per 10 power:")
pprint.pprint(power_10_list)
print("*****************************************")