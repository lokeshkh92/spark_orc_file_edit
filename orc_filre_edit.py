from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random

def filename(path):
    return path

#list from which value to be relaced in a column
pf = ['Forbidden','Non-Preferred','Preferred']

#Create spark session
spark = SparkSession.builder.appName("Edit orc file").getOrCreate()

#Input path for orc file
A = spark.read.orc('<basepath of orc file>')

# Replace value of a column with the regex
B = A.withColumn("<column_name>", regexp_replace("<column_name>", "<value to be replaced>", "<value which is to be replaced>"))

# Replace values in a column from a list of values mentioned above
C = B.withColumn("<column_name>",lit(random.choice(pf)))

#Save the dataframe as orc file alongwith partition column intact
C.write.partitionBy("<partition column>").format("orc").save('<basepath where orc file to be written>')
