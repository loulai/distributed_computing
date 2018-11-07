from pyspark import SparkConf, SparkContext

# Create SparkContext
conf = SparkConf().setMaster("local[*]").setAppName("ex04")
sc = SparkContext(conf=conf)

rdd = sc.textFile("./data/exampleNumbers.txt", 8) # default split per line
print(rdd.glom().collect())
converted_rdd = rdd.map(lambda x: int(x))


print("-------------------- Filtering:")

filtered_rdd = converted_rdd.filter(lambda x: x >= 8)
print(filtered_rdd.glom().collect())

print("-------------------- Reduce:")

reduced_rdd = filtered_rdd.count()
print(reduced_rdd)

#print(converted_rdd.glom().collect())

sc.stop()