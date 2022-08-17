##########################################################################
## streamingCSE545sp22_lastname_id.py  v1
## 
## Template code for assignment 1 part 1. 
## Do not edit anywhere except blocks where a #[TODO]# appears
##
## Student Name:
## Student ID: 


import math
import itertools
import sys
from pprint import pprint
from random import random
from collections import deque
from sys import getsizeof
import resource
from tokenize import group

import numpy as np

##########################################################################
##########################################################################
# Methods: implement the methods of the assignment below.  
#
# Each method gets 1 100 element array for holding ints of floats. 
# This array is called memory1a, memory1b, or memory1c
# You may not store anything else outside the scope of the method.
# "current memory size" printed by main should not exceed 8,000.

MEMORY_SIZE = 100 #do not edit

memory1a =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit

def task1ADistinctValues(element, returnResult = True):
    #[TODO]#
    #procss the element you may only use memory1a, storing at most 100
    """
    We are considering Flajolet-Martin algorithm to count the distinct elements. We are using 15 hash functions because 
    in the stream there will not be any element with more than 15 ditgits(my assumption). Then in hash function I am giving 
    weights and taking modulus in range if 2**32 beacuse the range of intergers in python is from -2**32 to 2**32 approx.
    """
    def trailingZeros(num):
        res=0
        num=num[2:]
        for i in range(len(num)-1,-1,-1):
            if num[i]=="0":
                res+=1
            else:
                break
        return res
    # We are creating static hash functions by giving fixed values of a and b
    def hashfunction(a,b,num):
        return (a*num+b)%(2**32)
    a,b=1,3
    num_hash=15
    for i in range(1,num_hash+1):
        temp=math.pow(2,trailingZeros(bin(hashfunction(a,b,element))))
        memory1a[i]=temp if memory1a[i] is None or temp>memory1a[i] else memory1a[i]
        a,b=a+18,b+24
    # store number of elements observed till now
    if memory1a[0] is None:
        memory1a[0] = 0
    memory1a[0]+=1
    if returnResult: #when the stream is requesting the current result
        result = 0
        step = int(math.log(num_hash))
        group_rs=[]
        for i in range(1,num_hash+1, step):
            end= i+int(math.log(num_hash))
            if end>num_hash+1:
                end=num_hash+1
            temp=list(itertools.islice(memory1a,i,end))
            group_rs.append(sum(temp)/len(temp))


        result=np.median(sorted(group_rs))

        # for i in range(0,numberofhashfunction,)
        #any additional processing to return the result at this point
        return result
    else: #no need to return a result
        pass


memory1b =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit

def task1BMedian(element, returnResult = True):
    #[TODO]#
    #procss the element
    """
    We are given with Pareto type 1 distribution
    lets store sum of log of elements at memory1b[0] and number of elemets in memory1b[1] for Alpha calculation
    for calculating median we have m=(lowest element)*pow(2,1/alpha). So storing the lowest lement in 1(which is given)
    """
    if memory1b[1] is None:
        memory1b[1] = 0
    memory1b[1]+=1
    if memory1b[0] is None:
        memory1b[0] = 0
    memory1b[0]+=math.log(element)
    memory1b[2]=element if (memory1b[2] is None) or (element<memory1b[2]) else memory1b[2]
    if returnResult: #when the stream is requesting the current result
        result = 0
        #[TODO]#
        #any additional processing to return the result at this point
        alpha=memory1b[1]/memory1b[0]
        result= (math.pow(2,1/alpha))
        return result
    else: #no need to return a result
        pass
    

memory1c =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit

def task1CMostFreqValue(element, returnResult = True):
    #[TODO]#
    #procss the element
    """
    Below we are trying to store elements in even positions and frequency of repective element is stored in next odd index
    """
    if element not in list(memory1c)[::2]:
        i=0
        if None in memory1c:
            while  memory1c[i] is not None:
                i+=1
                if i ==MEMORY_SIZE:
                    i=MEMORY_SIZE-2
                    break
        if memory1c[i+1] is None or memory1c[i+1]==1:
            memory1c[i]=element
            memory1c[i+1]=1
    else:
        for i in range(0,MEMORY_SIZE,2):
            if memory1c[i]==element:
                memory1c[i+1]+=1
                break


    if returnResult: #when the stream is requesting the current result
        result = 0
        #[TODO]#
        #any additional processing to return the result at this point
        maxi=1
        for i in range(1,MEMORY_SIZE,2):
            if memory1c[i] is not None and memory1c[maxi]<memory1c[i]:
                maxi=i
        result=memory1c[maxi-1]
        return result
    else: #no need to return a result
        pass


##########################################################################
##########################################################################
# MAIN: the code below setups up the stream and calls your methods
# Printouts of the results returned will be done every so often
# DO NOT EDIT BELOW

def getMemorySize(l): #returns sum of all element sizes
    return sum([getsizeof(e) for e in l])+getsizeof(l)

if __name__ == "__main__": #[Uncomment peices to test]
    
    print("\n\nTESTING YOUR CODE\n")
    
    ###################
    ## The main stream loop: 
    print("\n\n*************************\n Beginning stream input \n*************************\n")
    filename = sys.argv[1]#the data file to read into a stream
    printLines = frozenset([10**i for i in range(1, 20)]) #stores lines to print
    peakMem = 0 #tracks peak memory usage
    
    with open(filename, 'r') as infile:
        i = 0#keeps track of lines read
        for line in infile:
        
            #remove \n and convert to int
            element = int(line.strip())
            i += 1
            
            #call tasks         
            if i in printLines: #print status at this point: 
                result1a = task1ADistinctValues(element, returnResult=True)
                result1b = task1BMedian(element, returnResult=True)
                result1c = task1CMostFreqValue(element, returnResult=True)
                
                print(" Result at stream element # %d:" % i)
                print("   1A:     Distinct values: %d" % int(result1a))
                print("   1B:              Median: %.2f" % float(result1b))
                print("   1C: Most frequent value: %d" % int(result1c))
                print(" [current memory sizes: A: %d, B: %d, C: %d]\n" % \
                    (getMemorySize(memory1a), getMemorySize(memory1b), getMemorySize(memory1c)))
                
            else: #just pass for stream processing
                result1a = task1ADistinctValues(element, False)
                result1b = task1BMedian(element, False)
                result1c = task1CMostFreqValue(element, False)
                
            memUsage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if memUsage > peakMem: peakMem = memUsage
        
    print("\n*******************************\n       Stream Terminated \n*******************************")
    print("(peak memory usage was: ", peakMem, ")")