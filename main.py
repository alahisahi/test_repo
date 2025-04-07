from pyspark.sql import SparkSession
import findspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType
findspark.init()


# Create a Spark session
spark = SparkSession.builder \
    .appName("Simple Spark Program") \
    .getOrCreate()

# Create a simple DataFrame
schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("Address", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("qty", IntegerType(), True),
    StructField("Anount", DoubleType(), True),
    StructField("Unitprice", DoubleType(), True),
    StructField("Place", StringType(), True),
    StructField("Type", StringType(), True),
    StructField("power", DoubleType(), True),
])
df=spark.read.csv(r"C:\Users\alahi\Downloads\SampleCSVFile_11kb.csv",  schema=schema, header=True)

# Show the DataFrame
print(df.printSchema())
df.show()

# Stop the Spark session
