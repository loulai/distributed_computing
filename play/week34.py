from pyspark import SparkConf, SparkContext

# Create SparkContext
conf = SparkConf().setMaster("local[*]").setAppName("ex02-2")
sc = SparkContext(conf=conf)

rdd = sc.textFile("./data/exampleNumbers.txt", 8)

odd_numbers = rdd.map(lambda x: int(x)).filter(lambda x: x%2!=0)
print(odd_numbers.collect())