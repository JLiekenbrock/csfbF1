#1
print("#1a: wc Shakespeare.txt: 124456 bytes, 901325 lines, 5458199 words")
print('#1b: grep -c -i "by William Shakespeare" Shakespeare.txt: 38')

#2
print("""
The first part of the pipeline searches all occurences of "by William Shakespeare" in Shakespeare.txt.
The Parameter -B 6 further includes the 6 lines before the selected pattern.
The output of the first part is then matched again with a expression in three pieces:

The regex ^$ is denied by the option -e so all empty lines are excluded.
The regex tr '\n' ' ' is used to replace all newlines with a space.
The regex sed 's/ -- /\n/g' is used to replace all double dashes with a newline.

The execution involes 4 processes.
""")

print("#4")

from re import X
from turtle import xcor
from pandas import isnull
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col, lag, when, regexp_replace, count_distinct,countDistinct,spark_partition_id
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

sc = spark.sparkContext

print("#4 a\n")

df1 = spark.read.text("Shakespeare.txt") \
    .withColumn("rowId", monotonically_increasing_id())  \
    .filter(col("rowId") >245 )

df1.show()

print("#4 b\n")

copyright = '''<<THIS ELECTRONIC VERSION OF THE COMPLETE WORKS OF WILLIAM
SHAKESPEARE IS COPYRIGHT 1990-1993 BY WORLD LIBRARY, INC., AND IS
PROVIDED BY PROJECT GUTENBERG ETEXT OF ILLINOIS BENEDICTINE COLLEGE
WITH PERMISSION.  ELECTRONIC AND MACHINE READABLE COPIES MAY BE
DISTRIBUTED SO LONG AS SUCH COPIES (1) ARE FOR YOUR OR OTHERS
PERSONAL USE ONLY, AND (2) ARE NOT DISTRIBUTED OR USED
COMMERCIALLY.  PROHIBITED COMMERCIAL DISTRIBUTION INCLUDES BY ANY
SERVICE THAT CHARGES FOR DOWNLOAD TIME OR FOR MEMBERSHIP.>>'''

copyright = [line for line in copyright.split("\n")]

df1.filter(col("value").isin(copyright)).show()

df1=df1.filter(~col("value").isin(copyright))

df1.show()

print("#4 c\n")
from pyspark.sql.functions import row_number
from pyspark.sql import functions, Window
from operator import add

df1 = df1.withColumn("play", col("value").contains("by William Shakespeare") ) 

df1 = df1.withColumn("Lag", when(col("play"), lag(col("value"),2).over(
    Window.orderBy('rowId'))))
df1.show()

df1 = df1.withColumn("title", when(col("Lag")=="", lag(col("value"),3).over(
    Window.orderBy('rowId'))).otherwise(col("Lag")))
df1.show()

df1 = df1.withColumn("title", functions.last("title", ignorenulls=True).over(Window.orderBy("rowId")))

df1.show()

df1.filter(col("title")=="").show()


df1.groupBy("title").count().show()

df1.rdd.getNumPartitions()
df1=df1.drop("Lag","play","rowId").dropna()

df1.rdd.getNumPartitions()

partitions = df1.agg(countDistinct(col("title"))).collect()[0][0]

df1=df1.repartition(partitions,"title")

df1.rdd.getNumPartitions()

df1.withColumn("partitionId", spark_partition_id()).groupBy("partitionId").count().show()

df1.groupBy("title").count().show()

sc.parallelize(df1.collect()).map(lambda x : len(x)).reduce(add)

print('''
#7
Stopwords are common words that add no meaning to a text.
Therefore they are often removend during text mining.lines
A collection of stopword-lists is avaible in this repo:
https://github.com/stopwords-iso/stopwords-en/tree/master/raw

''')