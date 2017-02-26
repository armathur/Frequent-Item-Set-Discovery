from pyspark import SparkContext
from operator import add
from itertools import combinations, chain
import sys
from collections import defaultdict


sc = SparkContext(appName="son_algorithm")

def compareAll(buckets, itemSets):
    localSet_2 = defaultdict(int)
    _preFinalSet = set()
    record_2 = processFile(buckets)

    bucketList_2 = list()
    for bucket in record_2:
        bucket_trans = frozenset(bucket)
        bucketList_2.append(bucket_trans)

    for item in itemSets:
        for bucket in bucketList_2:
            if item.issubset(bucket):
                localSet_2[item] += 1

    toRetItems_2 = []
    for key, value in localSet_2.items():
        toRetItems_2.append((tuple(key),value))


    return toRetItems_2



def processFile(buckets):
    for line in buckets:
        line = line.strip().rstrip(',')  # Remove trailing comma
        record = frozenset(line.split(','))
        yield record



def retriveItemSet(buckets):
    itemSet = set()
    bucketList = list()
    for bucket in buckets:
        bucket_trans = frozenset(bucket)
        bucketList.append(bucket_trans)
        #print bucketList
        for item in bucket_trans:
            itemSet.add(frozenset([item]))
    return itemSet, bucketList

def findMinSupported(itemSet, bucketList, minSupport, freqSet ):
    #print "in here"
    _itemSet = set()
    localSet = defaultdict(int)

    for item in itemSet:
        for bucket in bucketList:
            if item.issubset(bucket):
                freqSet[item] += 1
                localSet[item] += 1

    for item, count in localSet.items():
        support = float(count) / len(bucketList)
        #print item, " > ",support
        if support >= minSupport:
            _itemSet.add(item)

    return _itemSet

def joinSet(itemSet, length):
    """Join a set with itself and returns the n-element itemsets"""
    return set([i.union(j) for i in itemSet for j in itemSet if len(i.union(j)) == length])

def a_priori(buckets, minSupport):
    record = processFile(buckets)
    itemSet, bucketList = retriveItemSet(record)
    #print bucketList
    freqSet = defaultdict(int)
    finalSet = dict()
    #the dictionary to hold itemsSets that satisfy minSupport

    oneCSet = findMinSupported(itemSet, bucketList, minSupport, freqSet)
    #print oneCSet
    currentLSet = oneCSet
    k = 2
    while (currentLSet != set([])):
        finalSet[k - 1] = currentLSet
        currentLSet = joinSet(currentLSet, k)
        currentCSet = findMinSupported(currentLSet,
                                                bucketList,
                                                minSupport,
                                                freqSet)
        currentLSet = currentCSet
        k = k + 1

    def getSupport(item):
        """local function which Returns the support of an item"""
        return float(freqSet[item]) / len(bucketList)


    toRetItems = []
    for key, value in finalSet.items():
        toRetItems.extend([(tuple(item), getSupport(item))
                           for item in value])
    return toRetItems

line = sc.textFile(sys.argv[1])
support_threshold = float(sys.argv[2])
outputFileName = sys.argv[3]

__sub_files__ = line.getNumPartitions()


itemSets = line.mapPartitions(lambda x: a_priori(x,support_threshold)).reduceByKey(lambda x,y: x).map(lambda x: x[0]).collect()

# print len(itemSets)
# for item in itemSets:
#     print item

localSet = set()
itemSets = (map(frozenset,itemSets))

for item in itemSets:
    localSet.add(item)
#print localSet

globalItemSet = line.mapPartitions(lambda y: compareAll(y,localSet)).reduceByKey(lambda x,y: x+y).collect()

for item in globalItemSet:
    if item[1]/float(line.count())>=support_threshold:
            with open(outputFileName, "a") as file_model:
                file_model.write((",".join(map(str,(item[0]))))+ "\n")



