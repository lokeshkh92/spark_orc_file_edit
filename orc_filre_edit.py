from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def filename(path):
    return path

sourceFile = udf(filename, StringType())
spark = SparkSession.builder.appName("Find FileName").getOrCreate()
A = spark.read.orc('<basepath of orc file>')
B = A.withColumn("<column_name>", regexp_replace("<column_name>", "<value to be replaced>", "<value which is to be replaced>"))
B.write.partitionBy("<partition column>").format("orc").save('<basepath where orc file to be written>')
