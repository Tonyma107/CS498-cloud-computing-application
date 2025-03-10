#!/usr/bin/env python

# Use LF instead of CRLF for end-of-line in files for Windows compatibility.
# Do not change the existing code as it may lead to unexpected errors.
'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]

with open(stopWordsPath) as f:
    stopwords = set(line.strip().lower() for line in f if line.strip())	#TODO

with open(delimitersPath) as f:
    delimiters = f.read().strip()#TODO

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)

def tokenize(line):
    for d in delimiters:
        line = line.replace(d, ' ') 
    return line.lower().split()
tokens = lines.flatMap(tokenize) \
              .filter(lambda word: word and word not in stopwords)
word_counts = tokens.map(lambda word: (word, 1)) \
                   .reduceByKey(lambda a, b: a + b)
collected_counts = word_counts.collect()
sorted_counts = sorted(collected_counts, key=lambda x: (x[1], x[0]))
top_10 = sorted_counts[-10:]
final_sorted = sorted(top_10, key=lambda x: x[0])

#TODO

outputFile = open(sys.argv[4],"w")

with outputFile as out:
    for word, count in final_sorted:
        line = f"{word}\t{count}\n"
        out.write(line)
#TODO
#write results to output file. Foramt for each line: (line +"\n")

sc.stop()
