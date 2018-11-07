from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("ex03")
sc = SparkContext(conf = conf)

text = sc.textFile("./data/exampleText.txt")
text.count()
print(text.count())

sc.stop()