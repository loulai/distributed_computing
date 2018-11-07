from pyspark import SparkConf, SparkContext

# Create SparkContext
conf = SparkConf().setMaster("local[*]").setAppName("ex02-1")
sc = SparkContext(conf=conf)

rdd = sc.textFile("./data/README.md")

print(rdd.union(rdd))

# Creating two RDDs from number arrays
x = sc.parallelize(range(1,6))
y = sc.parallelize(range(3,10))

print(x.cartesian(y).collect())
print()
print(x.cartesian(y).glom().collect()) # Q: why is everything in an individual array now?
# A: Becuase glom makes an array of each element in a partition?

"""
# original elements
print("------- ORIGINAL -------")
print(x.collect())
print(y.collect())

# return the union
print("------- union -------")
print(x.union(y).collect())

# return distinct set of the union
print("------- union.distinct() -------")
print(x.union(y).distinct().collect())

# subtact
# (not mathematical subtract. More like, remove from x elems that are in y)
print("------- subtract -------")
print(x.subtract(y).collect())

# sample
# withReplacement: if TRUE (replace), FALSE (don't replace) and max samples equal num elements
# fraction       : the chance for each element to be drawn. High means most will return.
# seed           : ensure consistency
print("------- sample -------")
print(x.sample(False, 0.1, 1).collect())
print(x.sample(False, 0.9, 1).collect()) # withReplacement=False, fraction=, seed=1
"""


