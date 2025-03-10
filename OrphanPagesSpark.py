#!/usr/bin/env python
# Use LF instead of CRLF for end-of-line in files for Windows compatibility.

import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)


def parse_line(line):
    line = line.strip()
    if not line:
        return ("", [])
    parts = line.split(':', 1)
    source = parts[0].strip()
    targets = parts[1].split() if len(parts) > 1 else []
    return (source, [t.strip() for t in targets if t.strip()])

lines = sc.textFile(sys.argv[1], 1)
parsed = lines.map(parse_line).filter(lambda x: x[0] != "")

sources = parsed.map(lambda x: x[0]).distinct()

targets = parsed.flatMap(lambda x: x[1]).filter(lambda t: t != "").distinct()
orphans = sources.subtract(targets)
sorted_orphans = orphans.sortBy(lambda x: x)
with open(sys.argv[2], "w") as out:
    for page in sorted_orphans.collect():
        out.write(f"{page}\n")

#TODO
#write results to output file. Foramt for each line: (line + "\n")

sc.stop()

