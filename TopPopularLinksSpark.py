#!/usr/bin/env python
# Use LF instead of CRLF for end-of-line in files for Windows compatibility.
# Do not change the existing code as it may lead to unexpected errors.

import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

def parse_line(line):
    line = line.strip()
    if not line:
        return []
    parts = line.split(':', 1)
    source = parts[0].strip()
    targets = parts[1].split() if len(parts) > 1 else []
    return [(t, 1) for t in targets if t.strip() != ""]#TODO

target_counts = lines.flatMap(parse_line) \
                     .reduceByKey(lambda a, b: a + b)

sorted_counts = target_counts.sortBy(lambda x: (x[1], x[0]))
top_10 = sorted_counts.takeOrdered(10, key=lambda x: (-x[1], x[0]))

final_sorted = sorted(top_10, key=lambda x: x[0])


with open(sys.argv[2], "w") as output:
    for page, count in final_sorted:
        output.write(f"{page}\t{count}\n")
#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()

