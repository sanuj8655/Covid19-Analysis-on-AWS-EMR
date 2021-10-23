import pyspark

from pyspark.sql.functions import *

from pyspark.context import SparkContext

from pyspark.sql import SQLContext

from pyspark.sql.session import SparkSession

sc = SparkContext()

sqlContext = SQLContext(sc) 

spark = SparkSession.builder.master("local").appName("sample").getOrCreate()

df = spark.read.parquet("s3://covid-data1/cowid.parquet/")

df.createOrReplaceTempView("final_data") 

sqlContext.sql("create table cowid_table as select * from final_data")