from pyspark import SparkConf, SparkContext

# Create SparkContext
conf = SparkConf().setMaster("local[*]").setAppName("ex04")
sc = SparkContext(conf=conf)

rdd = sc.textFile("./data/exampleText.txt")

word_counts = rdd.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)

print(word_counts.glom().collect())
print(rdd.glom().collect())
sc.stop()

