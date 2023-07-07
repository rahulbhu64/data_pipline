from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder \
    .appName("Rahul ") \
    .config("spark.jars", "/usr/share/java/postgresql.jar") \
    .getOrCreate()

# Source database connection properties
src_db_properties = {
    "url": "jdbc:postgresql://localhost:5432/11Aug",
    "driver": "org.postgresql.Driver",
    "user": "postgres",
    "password": "xyz"
}

# Read data from the source database table
source_df = spark.read.jdbc(
    url=src_db_properties["url"],
    table="hr_employee",
    properties=src_db_properties
)

# print all columns name or root schema
source_df.printSchema()
# collecting the data as a list
result = source_df.collect()
# converting the all data as row
rows = [Row(*r) for r in result]

# Created a DataFrame and hold the all data as row and column :

target_df = spark.createDataFrame(rows, source_df.schema)

# connection with target table
target_db_properties = {
    "url": "jdbc:postgresql://localhost:5432/Rahul",
    "driver": "org.postgresql.Driver",
    "user": "postgres",
    "password": "xyz",
    "dbtable": "employee"
}
# loading the data in targeted database
target_df.write \
    .format("jdbc") \
    .option("url", target_db_properties["url"]) \
    .option("driver", target_db_properties["driver"]) \
    .option("user", target_db_properties["user"]) \
    .option("password", target_db_properties["password"]) \
    .option("dbtable", target_db_properties["dbtable"]) \
    .mode("overwrite") \
    .save()


# Stop the SparkSession
spark.stop()
