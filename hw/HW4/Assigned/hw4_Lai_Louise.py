from pyspark import SparkContext, SparkConf
from user_definition import *
import re
# Do not add any additional libraries.


def getRange(begin_int, end_int):
    if(begin_int == end_int):  # e.g. 8AM to 8AM is 24 hours
        return(set(range(0, 24)))
    elif(begin_int < end_int):
        return(set(range(begin_int, end_int)))
    elif(begin_int > end_int):
        # between [20-24 + 0-2]
        return set(range(begin_int, 24)).union(set(range(0, end_int)))


def convertTo24(string):
    int_part = int(re.findall(r"\d+", string)[0])
    suffix_part = re.findall(r"[AP]M", string)[0]

    if(suffix_part == "PM" and int_part < 12):  # < 12 is for 12PM not to be 24
        return(int_part + 12)
    elif(suffix_part == "AM" and int_part == 12):
        return(0)
    elif(suffix_part == "AM" or int_part == 12):
        return(int_part)
    else:
        return("NEITHER")  # this should never, ever happen!


def isBetweenTime(begin, end, given_range):
    each_row_start_time = convertTo24(begin)
    each_row_end_time = convertTo24(end)
    each_row_range = getRange(each_row_start_time, each_row_end_time)
    return given_range.issubset(each_row_range)


def main():
    # Set up spark context
    conf = SparkConf().setAppName(app_name)
    sc = SparkContext(conf=conf)

    total_lines = sc.textFile(input_file, 4)

    # Subtract header
    headerRDD = sc.parallelize([total_lines.first()])
    lines = total_lines.subtract(headerRDD)

    # 1) total number of lines
    num_total_lines = len(total_lines.collect())
    q_1 = "Total number of lines in the input file:{}\n"\
        .format(num_total_lines)

    # 2) total number of unique business information
    lines_cleaned = lines.distinct().collect()  # -header, distinct
    num_total_uniq = len(lines_cleaned)
    q_2 = "Total Number of unique business information:{}\n"\
        .format(num_total_uniq)

    # 3) unique business information for each day
    regex = r",(?=\S)"
    lines_split = sc.parallelize(
            [re.split(regex, row) for row in lines_cleaned], 6)

    day_string = ["Sunday", "Monday", "Tuesday", "Wednesday",
                  "Thursday", "Friday", "Saturday"]
    final_days = []
    for i, day_str in enumerate(day_string):
        num_days = len(lines_split.filter(lambda x: int(x[0]) == i).collect())
        if num_days > 0:
            final_days.append((num_days, day_str))

    # sort by count, and then by alphabet
    final_days_sorted = sorted(final_days, key=lambda x: (-x[0], x[1]))
    # [print(d) for d in final_days_sorted]

    # 4) Between time
    header_array = headerRDD.collect()[0].split(",")

    # filter for day
    day_rdd = lines_split.filter(lambda x: x[1] == day)

    # get time range
    given_time_range = getRange(convertTo24(start_time), convertTo24(end_time))

    # create rdd for filtered rows
    between_times_rdd = day_rdd.filter(
        lambda row: isBetweenTime(row[2], row[3], given_time_range))

    q_4 = \
        "The number of business open on {} between {} and {}:{}"\
        .format(day, start_time, end_time, str(between_times_rdd.count()))

    # write output to file
    with open("./output.txt", "w") as file:
        file.write(q_1)
        file.write(q_2)
        for d in final_days_sorted:
            file.write("{}:{}\n".format(d[1], d[0]))
        file.write(q_4)
    sc.stop()


if __name__ == "__main__":
    main()
