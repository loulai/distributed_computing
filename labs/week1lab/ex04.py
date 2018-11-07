from pyspark import SparkConf, SparkContext

# Create SparkContext.
conf = SparkConf().setMaster("local[*]").setAppName("ex04")
sc = SparkContext(conf=conf)

# Load Data.
lines = sc.textFile("../Data/README.md")
count = lines.count()
first = lines.first()

# Write to a file.
output_file = "ex04_output.txt"
f = open(output_file, "w")
f.write(str(count))
f.write("\n")
f.write(first)

sc.stop()
