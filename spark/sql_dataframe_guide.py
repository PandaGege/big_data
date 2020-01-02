# coding:utf8

from __future__ import print_function
import sys
import time

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

master = "spark://cluster.master:7077"
# master = "local[2]"
spark = SparkSession.builder \
        .master(master) \
        .appName('SQL Demo') \
        .getOrCreate()

print('it works well')

people = spark.read.csv("/data/people.txt", sep=",", header=True,
                        inferSchema=True)
department = spark.read.csv("/data/department.txt", sep=",", header=True,
                            inferSchema=True)
case1 = people.filter(people.age > 30) \
        .join(department, people.deptId == department.id) \
        .groupBy(department.desc, "gender") \
        .agg({"salary": "avg", "age": "max"}) \
        .collect()
print("the result is: ", case1)

# agg(*exprs)
max_age = people.agg({'age': "max"}).collect()
min_age = people.agg(F.min(people.age)).collect()
print('the max age is :', max_age)
print('the min age is :', min_age)

# alias(alias)

# cache()

# people.cache()

# checkpoint(eager=True)

# coalesce(numPartitions)
print('the num of partitions:', people.rdd.getNumPartitions())
print('after coalesce the num of partitions:', people.coalesce(2).rdd.
                                                    getNumPartitions())
# colRegex(colName)
people.select(people.colRegex("`.*a.*`")).show()

# collect()

# .columns

# corr(col1, col2, method=None)
print("Pearson Correlation Coefficient: ", people.corr('age', 'salary'))

# cov(col1, col2)
print('sample covariance: ', people.cov('age', 'salary'))

# count()
print('num of rows: ', people.count())

# createGlobalTempView(name)
people.createGlobalTempView('people')
people_tmp = spark.sql('select * from global_temp.people')
same = sorted(people.collect()) == sorted(people_tmp.collect())
print('dateframe is the same of temp:', same)

# createOrReplaceGlobalTempView(name)
# createOrReplaceTempView(name)
# createTempView(name)
"""
`createGlobalTempView`和`createOrReplaceGlobalTempView`的生命周期是spark
application.
`createOrReplaceTempView`和`createTempView`的生命周期是该spark session
"""

# crossJoin(other) 笛卡尔积

# crosstab(col1, col2) 级联表
print(people.crosstab('deptId', 'gender').collect())
"""
[Row(deptId_gender=u'2', female=1, male=1),
 Row(deptId_gender=u'1', female=0, male=3),
  Row(deptId_gender=u'3', female=2, male=0)]
"""

# cube(*cols)
people.cube('name', people.age).count().orderBy('name', 'age').show()
"""
+-------+----+-----+
|   name| age|count|
+-------+----+-----+
|   null|null|    7|
|   null|  12|    1|
|   null|  19|    1|
|   null|  29|    2|
|   null|  30|    1|
|   null|  31|    1|
|   null|  37|    1|
|   Andy|null|    1|
|   Andy|  30|    1|
| Fakker|null|    1|
| Fakker|  29|    1|
| Justin|null|    1|
| Justin|  19|    1|
|  Killy|null|    1|
|  Killy|  12|    1|
|Michael|null|    1|
|Michael|  29|    1|
|    Pop|null|    1|
|    Pop|  31|    1|
|  Regan|null|    1|
+-------+----+-----+
"""

# describe(*cols)
people.describe(['age']).show()
"""
+-------+------------------+
|summary|               age|
+-------+------------------+
|  count|                 7|
|   mean|26.714285714285715|
| stddev| 8.380817098475257|
|    min|                12|
|    max|                37|
+-------+------------------+
"""

# distinct()
people.distinct().count()

# drop(*cols)

# dropDuplicates(subset=None) == drop_duplicates(subset=None)
# 删除重复行。subset参数为仅考虑其中的一些列

# dropna(how='any', thresh=None, subset=None)
'''
how: 'any' or 'all'
thresh: 至少保留这个非空列
subset: 仅考虑subset指定的列
'''
student = spark.sparkContext.parallelize([\
               Row(name="Alice", age=10, height=100),
               Row(name="Bob", age=12, height=None),
               Row(name="Regan", age=None, height=None)]).toDF()
student.dropna(thresh=3).show()
"""
+---+------+-----+
|age|height| name|
+---+------+-----+
| 10|   100|Alice|
+---+------+-----+
"""
student.dropna(thresh=2).show()
"""
+---+------+-----+
|age|height| name|
+---+------+-----+
| 10|   100|Alice|
| 12|  null|  Bob|
+---+------+-----+
"""

# exceptAll(other)
df1 = spark.createDataFrame(
        [("a", 1), ("a", 1), ("a", 1), ("a", 2), ("b",  3), ("c", 4)],
        ["C1", "C2"])
df2 = spark.createDataFrame([("a", 1), ("b", 3)], ["C1", "C2"])
df1.exceptAll(df2).show()
"""
+---+---+
| C1| C2|
+---+---+
|  a|  1|
|  a|  1|
|  a|  2|
|  c|  4|
+---+---+
"""

spark.stop()
