from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
# Creates Empty RDD
empty_rdd = spark.sparkContext.emptyRDD()
print(empty_rdd, "Empty RDD .......")

# Creates Empty RDD USING Parallelize
# rdd = spark.sparkContext.parallelize([])

# Create Empty DataFrame with Schema (StructType)

# Create Schema
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

schema = StructType([
    StructField('first_name',StringType(),True),
    StructField('middle_name',StringType(),True),
    StructField('last_name',StringType(),True),
])

df = spark.createDataFrame(empty_rdd,schema)
df.show()

df3 = spark.createDataFrame([], StructType([]))
df3.printSchema()

import pandas as pd


# data = [(1, 'John'), (2, 'Jane'), (3, 'Smith')]
# df = spark.createDataFrame(data, ['id', 'name'])
# df.show()

data1 = {'id': [1, 2, 3],
        'name': ['John', 'Jane', 'Smith'],
        'age': [25, 30, 35]}

# Define the schema explicitly to avoid type inference issues
schema = StructType([
    StructField('id', IntegerType(), nullable=False),
    StructField('name', StringType(), nullable=False),
    StructField('age', IntegerType(), nullable=False)
])

# Create the DataFrame with the specified schema
df1 = spark.createDataFrame(list(zip(data1['id'], data1['name'], data1['age'])), schema=schema)

# Show the DataFrame
df1.show()


# # If you have a pandas DataFrame
# pandas_df = pd.DataFrame(data, columns=['id', 'name'])
# df = spark.createDataFrame(pandas_df)
# df.show()

df_csv = spark.read.csv('/home/shivam/worldstats.csv', header=True, inferSchema=True)
df_csv.printSchema()
df_csv.show()