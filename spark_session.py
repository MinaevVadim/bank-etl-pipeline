from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("app").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")