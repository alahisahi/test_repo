from pyspark.sql import SparkSession
import findspark
findspark.init()


# Create a Spark session
spark = SparkSession.builder \
    .appName("Simple Spark Program") \
    .getOrCreate()

# Create a simple DataFrame
df=spark.read.csv(r"C:\Users\alahi\Downloads\SampleCSVFile_11kb.csv",  inferSchema=True)

# Show the DataFrame
print(df.printSchema())
df.show()

# Stop the Spark session
