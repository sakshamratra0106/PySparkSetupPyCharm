# https://medium.com/@sanjumani619/pyspark-interview-questions-a840d9b0670
# Solution
# https://drive.google.com/drive/folders/1RMjGmbUTC9i8QUNDC9n5buRvY0rAcTWy

import sys
import time
from operator import add

from pyspark.sql import SparkSession

if __name__ == "__main__":

    print(sys.argv)

    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    # Question 1
    spark = SparkSession \
        .builder \
        .appName("PythonWordCount") \
        .config("spark.executor.processTreeMetrics.enabled", "false") \
        .getOrCreate()

    lines = spark.read.text("C:/Users/SAKSHAM/PycharmProjects/SparkHelloWorld/question1").rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)

    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    # Question 2
    lines = spark.read.text("C:/Users/SAKSHAM/PycharmProjects/SparkHelloWorld/question2")
    lines = lines.filter(lines['value'] != lines.first()[0])
    from pyspark.sql.functions import split

    lines = lines.withColumn("Name", split(lines['value'], "~")[0])
    lines = lines.withColumn("Age", split(lines['value'], "[|]")[1])
    lines = lines.drop("value")
    lines.show()

    # Question 3
    lines = spark.read.option("delimiter", "|").csv("C:/Users/SAKSHAM/PycharmProjects/SparkHelloWorld/question3",
                                                    header=True)
    from pyspark.sql.functions import *

    lines = lines.select("*", posexplode_outer(split("Education", ",")))
    lines.select("Name", "Age", "col", "pos")
    lines.select("Name", "Age", "col", "pos").show(truncate=False)
    lines.show()

    # Question 4
    lines = spark.read.csv("C:/Users/SAKSHAM/PycharmProjects/SparkHelloWorld/question4", header=True)
    lines.withColumn("amount_sub",
                     when(col("Transaction Type") == "debit", -1 * col("Amount")).otherwise(col("Amount"))).groupby(
        "Customer_No").agg(sum("amount_sub")).show(truncate=False)

    # Question 6
    lines = spark.read.option("delimiter", "|").csv("C:/Users/SAKSHAM/PycharmProjects/SparkHelloWorld/question6",
                                                    header=True)
    from pyspark.sql.window import Window
    from pyspark.sql.functions import *

    windowSpec = Window.partitionBy("Sub").orderBy(lines["Marks"].desc())
    lines.withColumn("ranked", rank().over(windowSpec)).filter(col("ranked") == 1).show(truncate=False)

    spark.stop()
