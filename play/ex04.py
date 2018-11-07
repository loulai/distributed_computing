from pyspark import SparkConf, SparkContext

# Create SparkContext
conf = SparkConf().setMaster("local[*]").setAppName("ex04") # why set master?
sc = SparkContext(conf=conf)

# Load Data
lines = sc.textFile("./data/exampleText.txt")
count = lines.count()
firstLine = lines.first()

# Write to file
output_file = "./data/ex04_output.txt"
f = open(output_file, "w")
f.write(str(count)) # convert to string before appending? Why?
f.write("\n")
f.write(firstLine)

sc.stop()
