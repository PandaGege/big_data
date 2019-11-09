# coding:utf8

from __future__ import print_function

import sys
import numpy as np
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.conf import SparkConf

appName = "HelloRDD"
master = "spark://cluster.master:7077"
# master = "local[1]"
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

# data = np.arange(12)
# distData = sc.parallelize(data)
# res = distData.reduce(lambda a, b: a + b)
# print("the result of reduce: %d"%res)

# # save as 
# rdd = sc.parallelize(range(1, 4)).map(lambda x: (x, "a"*x))
# rdd.saveAsSequenceFile('/data/rdd.sequence.file')
# 
# # read
# lines = sc.sequenceFile('/data/rdd.sequence.file')
# lineLengths = lines.map(lambda s: len(s)) # not computed, lazy load
# totalLength = lineLengths.reduce(lambda a, b: a+b)
# print('the length of data: %s'%totalLength)


# 全局变量， 在spark里不work. 因为foreach是在不同节点上并行计算元素
# driver进程获取不到其它节点的变量, 使用 Accumulator 代替
# 从标准输出打印结果时，也是同样的道理。foreach等并不是从driver的标准输出打印
# 在driver进程里，使用rdd.collect().foreach(println)打印(内存有可能溢出)
# rdd.take(100).foreach(println)
data = np.arange(12)
rdd = sc.parallelize(data)
counter = 0

def increment_counter(x):
    global counter
    counter += 1
rdd.foreach(increment_counter)

print('Counter value', counter) # !!! the result is zero

# try accumulator
accum = sc.accumulator(0)
rdd.foreach(lambda x: accum.add(x))
print('accum value', accum.value) # it works well


# Working with Key-Value Pairs
#  grouping or aggregating the elements by a key.
data = (('c', 2), ('a', 4), ('f', 31), ('z', 12), ('c', 321))
rdd = sc.parallelize(data)
res = rdd.sortByKey().collect()
print(res) # [('a', 4), ('c', 2), ('c', 321), ('f', 31), ('z', 12)]

res = rdd.groupByKey().mapValues(sum).collect()
print(res) # [('a', 4), ('c', 323), ('z', 12), ('f', 31)]


# Some Transformations
# map(func)

# filter(func)
rdd = sc.parallelize(range(10))
print(rdd.filter(lambda x: x % 2 == 0).collect()) # [0, 2, 4, 6, 8]

# flatMap(func)
# >>> rdd = sc.parallelize([2, 3, 4])
# >>> sorted(rdd.flatMap(lambda x: range(1, x)).collect())
# [1, 1, 1, 2, 2, 3]
# >>> sorted(rdd.flatMap(lambda x: [(x, x), (x, x)]).collect())
# [(2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4)]

# mapPartitions
rdd = sc.parallelize(range(10), 2)
def f(iterator): yield sum(iterator)
print(rdd.mapPartitions(f).collect()) # [10, 35]

# mapPartitionsWithIndex(func)
# sample(withReplacement, fraction, seed)
# union(otherDataset)
# intersection(otherDataset)
# distinct([numPartitions]))    
# groupByKey([numPartitions])
# reduceByKey(func, [numPartitions])
# aggregateByKey(zeroValue)(seqOp, combOp, [numPartitions])
# sortByKey([ascending], [numPartitions])
# join(otherDataset, [numPartitions])
# cogroup(otherDataset, [numPartitions])
# cartesian(otherDataset)
# pipe(command, [envVars])
# coalesce(numPartitions)
# repartition(numPartitions)
# repartitionAndSortWithinPartitions(partitioner)


# Some Actions
# reduce(func)
# collect() 
# count()
# first()
# take(n)
# takeSample(withReplacement, num, [seed])
rdd = sc.parallelize(range(10), 2)
sample_rdd = rdd.takeSample(False, 5)
print(sample_rdd)

# saveAsTextFile(path)
# saveAsSequenceFile(path)
# saveAsObjectFile(path)
# countByKey()
# foreach(func)


# shuffle
