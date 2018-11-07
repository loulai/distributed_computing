from pyspark import SparkContext, SparkConf
from user_definition import *
# Do not add any additional libraries.

# Create SparkContext
conf = SparkConf().setAppName(app_name)
sc = SparkContext(conf=conf)

# Load data
lines = sc.textFile(input_file, partition_ct)

# Define variables
sum = 0
listOfLines = []

# Get number of lines in each partition
for partition in lines.glom().collect():
    print(partition.count())
    #for k, line in enumerate(partition):
    #    pass
    listOfLines.append(k+1)
    sum = (k+1) + sum


# Write to output file
#with open("./output.txt", "w") as file:
#    file.write(str(sum) + "\n")
#    for elem in listOfLines:
#        file.write(str(elem) + "\n")

sc.stop()
