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
from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("NLP_Pipeline")\
        .getOrCreate()

    threads = 2

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print("Pi is roughly %f" % (4.0 * count / n))

    spark.stop()