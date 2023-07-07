from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
list1 = [2,3,4,5]
rdd = spark.sparkContext.parallelize(list1)
rdd_map = rdd.map(lambda x:x*2)
result = rdd_map.collect()
print(result)