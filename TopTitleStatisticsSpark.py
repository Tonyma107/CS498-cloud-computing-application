#!/usr/bin/env python
# Use LF instead of CRLF for end-of-line in files for Windows compatibility.
# Do not change the existing code as it may lead to unexpected errors.


import math
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

#TODO
counts = lines.map(lambda line: int(line.strip().split('\t')[1])).collect()
sum_counts = sum(counts)
mean_count = sum_counts // len(counts)
min_count = min(counts)
max_count = max(counts)
variance = sum((x - mean_count) ** 2 for x in counts) // len(counts)
with open(sys.argv[2], "w") as outputFile:
    outputFile.write(f"Mean\t{mean_count}\n")
    outputFile.write(f"Sum\t{sum_counts}\n")
    outputFile.write(f"Min\t{min_count}\n")
    outputFile.write(f"Max\t{max_count}\n")
    outputFile.write(f"Var\t{variance}\n")

'''
TODO write your output here
write results to output file. Format
outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % ans5)
'''

sc.stop()

