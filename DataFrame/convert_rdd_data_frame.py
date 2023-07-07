from pyspark.sql import SparkSession
#convert rdd to data frame

#1 creating rdd
spark = SparkSession.builder.getOrCreate()
dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
rdd = spark.sparkContext.parallelize(dept)
# 2 converting rdd to data frame usng toDF()
df = rdd.toDF(['department', 'marks'])
df.printSchema()
df.show()


dict = {
    'name':['Rahul', 'Anhad', 'Prince', 'Raja Babu', 'Shivam'],
    'AGE':[4, 4, 4, 4,4],
    'name':['Rahul', 'Anhad', 'Prince', 'Raja Babu', 'Shivam']
}