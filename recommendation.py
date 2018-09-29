dimport sys
from pyspark import SparkConf, SparkContext

####################
# HELPER FUNCTIONS #
####################


def funcname(network):
    """
    Parses a line from the data into users and lists of friends.
    """
    data = network.map(lambda line: line.split("\t")) # split by tab
    if data[1] != ''
        user = int(data[0])
        friends = data[1].strip()
    else 
        #code 
    user = int(data[0])
    friends = data[1].strip()
    return user, friends


################
# MAIN PROGRAM #
################

conf = SparkConf()
sc = SparkContext(conf=conf)
filename = "sociNet.txt"

network = sc.textFile(filename)