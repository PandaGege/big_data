# coding:utf8
"""
copy hive`s hive-site.xml hadoop`s hdfs-site.xml to conf/
"""

from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession \
        .builder \
        .appName("Python Spark SQL Hive integration example") \
        .enableHiveSupport() \
        .getOrCreate()

sql = "select userid, movieid, rating, unixtime from `u_data` limit 100"
sqlDF = spark.sql(sql)
stringsDS = sqlDF.rdd.map(lambda row: "userid: %d, movieid: %d, rating: %f, "
                          "unixtime: %s"%(row.userid, row.movieid, row.rating,
                          row.unixtime))
for record in stringsDS.collect():
    print(record)


# copy from examples/src/main/python/sql/hive.py
# You can also use DataFrames to create temporary views within a SparkSession.
Record = Row("key", "value")
recordsDF = spark.createDataFrame([Record(i, "val_" + str(i)) for i in range(1, 101)])
recordsDF.createOrReplaceTempView("records")

# Queries can then join DataFrame data with data stored in Hive.
spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
# +---+------+---+------+
# |key| value|key| value|
# +---+------+---+------+
# |  2| val_2|  2| val_2|
# |  4| val_4|  4| val_4|
# |  5| val_5|  5| val_5|
# ...
# $example off:spark_hive$

spark.stop()
