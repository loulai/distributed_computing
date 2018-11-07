from pyspark import SparkContext, SparkConf
from user_definition import *
# Do not add any additional libraries.

# Create SparkContext
conf = SparkConf().setAppName(app_name)
sc = SparkContext(conf=conf)

# Load Data
lines = sc.textFile(input_file, partition_ct)

with open("./output.txt", "w") as file:
    for i, partition in enumerate(lines.glom().collect()):
        file.write("-- Partition {} --\n".format((i + 1)))
        for line in partition:
            file.write("{}\n".format(line))

sc.stop()

