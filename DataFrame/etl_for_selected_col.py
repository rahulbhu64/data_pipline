from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


# creation of SparkSession
spark = SparkSession.builder \
        .appName('ETL For Selected Column') \
        .config("spark.jars", "/usr/share/java/postgresql.jar") \
        .getOrCreate()

# Source database connection properties
src_db_properties = {
    "url": "jdbc:postgresql://localhost:5432/11Aug",
    "driver": "org.postgresql.Driver",
    "user": "postgres",
    "password": "shivam"
}

# Reading the data from source table

source_df = spark.read.jdbc(
    url=src_db_properties["url"],
    table="hr_leave",
    properties=src_db_properties
)

# collecting the data from source table
result = source_df.collect()
# required column selection
selected_columns = ["id", "message_main_attachment_id", "private_name", "state"]
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("message_main_attachment_id", IntegerType(), nullable=True),
    StructField("private_name", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True)
])
# selected_data = [Row(*[row[col] for col in selected_columns]) for row in result]
selected_data = [Row(*[row[col] for col in selected_columns]) for row in result if row["id"] > 400 and row["state"] >= "validate"]

selected_df = spark.createDataFrame(selected_data, schema)

target_db_properties = {
    "url": "jdbc:postgresql://localhost:5432/Rahul",
    "driver": "org.postgresql.Driver",
    "user": "postgres",
    "password": "shivam",
    "dbtable": "leave"
}

selected_df.write \
    .format("jdbc") \
    .option("url", target_db_properties["url"]) \
    .option("driver", target_db_properties["driver"]) \
    .option("user", target_db_properties["user"]) \
    .option("password", target_db_properties["password"]) \
    .option("dbtable", target_db_properties["dbtable"]) \
    .mode("overwrite") \
    .save()

print('MAHADEV Success')