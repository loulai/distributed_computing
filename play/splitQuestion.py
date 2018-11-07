""" Filtering and Splitting

The following code demonstrates that there are two different outputs
that can be generated when filtering for the word 'spark'.

I would like to know why this difference exists, and what
is happening in the background.
"""
from pyspark import SparkConf, SparkContext

# Create SparkContext
conf = SparkConf().setMaster("local[*]").setAppName("ex02-1")
sc = SparkContext(conf=conf)

rdd = sc.textFile("./data/README.md")

# returns a filter of only lines with 'spark'
print("---------- FILTER  ----------")
print(rdd.filter(lambda x: 'spark' in x.lower()).collect())

# returns a filter of only lines with 'spark' (SPLIT)
print("---------- FILTER on SPLIT ----------")
print(rdd.filter(lambda x: 'spark' in x.lower().split(" ")).collect())
