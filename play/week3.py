from pyspark import SparkConf, SparkContext

# Create SparkContext
conf = SparkConf().setMaster("local[*]").setAppName("ex02-1")
sc = SparkContext(conf=conf)

rdd = sc.textFile("./data/README.md")

# this gives an array
# each list is a line
# each line contains multiple words
print("------------ MAP ------------")
print(rdd.map(lambda x: x.split()).collect())


# this gives one array with
# a flat structure
print("---------- FLATMAP ----------")
print(rdd.flatMap(lambda x: x.split()).collect())

# every line evaluates to true, so accept every line,
# then print it
print("---------- FILTER == TRUE ----------")
print(rdd.filter(lambda x: True).collect())


# returns a filter of only lines with 'spark'
print("---------- FILTER == spark ----------")
print(rdd.filter(lambda x: 'spark' in x.lower()).collect())


# returns a filter of only lines with 'spark' (SPLIT)
print("---------- FILTER on split == spark ----------")
print(rdd.filter(lambda x: 'spark' in x.lower().split(" ")).collect())