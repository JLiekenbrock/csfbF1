#1
print("#1a: wc Shakespeare.txt: 124456 bytes, 901325 words, 5458199 lines")
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

#3
print("""
    0: val textFile = sc.wholeTextFiles("my/path/*.csv", 8)
    a: df.filter($line > 245).show(false)
    b: d.filter(col("alphanumeric")
        .rlike("<<.+>>")
        ).show(false)
    c:
    d:
    e:

"""
)
#5

from re import X
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col, lag, when

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
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

df1=df1.filter(~col("value").isin(copyright)).show()

print("#4 c\n")

df1.withColumn("play", col("value").contains("by William Shakespeare") ) 

