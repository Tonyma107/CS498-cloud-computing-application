#!/usr/bin/env python
# Use LF instead of CRLF for end-of-line in files for Windows compatibility.
# Do not change the existing code as it may lead to unexpected errors.

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)


linksPath = sys.argv[1]
leaguePath = sys.argv[2]
outputPath = sys.argv[3]


lines = sc.textFile(linksPath, 1)

def parse_line(line):
    parts = line.split(':', 1)
    source = parts[0].strip()
    targets = parts[1].split() if len(parts) > 1 else []
    targets = [t for t in targets if t != source]
    return source, targets

parsedRDD = lines.map(parse_line)
targetPairs = parsedRDD.flatMap(lambda x: [(t, 1) for t in x[1]])
targetCounts = targetPairs.reduceByKey(lambda a, b: a + b)
inlinkDict = targetCounts.collectAsMap()

leaguePages = sc.textFile(leaguePath, 1) \
                .map(lambda line: line.strip()) \
                .filter(lambda line: line != "") \
                .collect()




filteredLeaguePages = [page for page in leaguePages if page in inlinkDict]
league_counts = {page: inlinkDict[page] for page in filteredLeaguePages}


results = []
for page in filteredLeaguePages:
    current_count = league_counts[page]
    rank = sum(1 for other_page in league_counts if league_counts[other_page] < current_count)
    results.append((page, rank))

results_sorted = sorted(results, key=lambda x: x[0])

with open(outputPath, "w") as out:
    for page, rank in results_sorted:
        out.write(f"{page}\t{rank}\n")

sc.stop()



#TODO
#write results to output file. Foramt for each line: (key + \t + value +"\n")

sc.stop()



