from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession
from src import utils


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    sc = SparkContext()

    spark = SparkSession(sc)\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')).\
        map(lambda x: (x, 1)).\
        reduceByKey(lambda a,b: utils.addList(a,b))

    print(counts.collect())

    output = counts.map(lambda line: utils.process(line) )

    output = output.collect()

    print(output)
    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()
