from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("TesteSpark") \
    .getOrCreate() 
print("Spartk funcionando!")
spark.stop()