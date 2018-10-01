
import sys
import csv
from pyspark import SparkConf, SparkContext

####################
# HELPER FUNCTIONS #
####################

def getKey(word):
    """
    Used to sort
    """
    return word[0]

def createCSV(obj, filename):
    """
    Create csv file
    """
    file = open(filename + ".csv", "w")
    writer = csv.writer(file, dialect="excel")
    writer.writerows(obj)
    file.close()


def parseData(network):
    """
    Read a line from the initial text file parses it into a user and lists of friends.
    Any line that has a user and friends would be reformatted.
    An example of such an operation on a line is changing:
    '9   0,6085,18972,19269'  to  ('9', ['0', '6085', '18972', '19269'])
    Each line gets turned from a string to Tuple(str, List[str])
    """
    data = network.map(lambda line: line.split("\t")) # split by tab
    data = data.map(lambda line: (line[0], line[1].split(',')))

    return data


def users_to_friend(line):
    """
    Maps every line of (user, [friend1, friend2, friend3 ...])
    to multiple objects of ((user, friend), value).
    If the value is 0, the user and friend are already friends.
    If the value is 1, the user and friend are not friends.
    """

    user = line[0]
    friends = line[1]

    friendships = []

    for friend in friends:
        friendships.append(((user, friend),0)) # direct friendships

    for friend1 in friends:
       for friend2 in friends:
           if(friend1 != '' and friend2 != ''):  
                if friend1 != friend2:
                    friendships.append(((friend1, friend2), 1)) #indirect friendships

    return friendships

def sumMutualFriends(values):
    """
    Sums all of the values, which aren't 0,
    that have the same key and come from indirect friendships 
    """
    for value in values:
        if value == 0:
            return 0
        else:
            return sum(values)

# Can't use below function because of function object not iretable error 
# def makerecommendation(pair_count):
#    return lambda pair_count: (pair_count[0][0], (pair_count[0][1], pair_count[1]))


def sortRecs(objects):
    """
    Returns a list of the top ten recommended friends for each unique user 
    """
    # Sorting strings rather than numbers,therefore it's sorting in alphabetical order 
    # And larger negative numbers preceed smaller negative numbers 
    # Outputs the top 10 pairs (user, count) 
    sorted_recommendation = sorted(objects, key = lambda count: (-1 * count[1], count[0]))[:10]  #only top 10
    recommended_friends = []

    # Adds the user of each pair into a list 
    for pair in sorted_recommendation:
        recommended_friends.append(pair[0])

    # return list of recommended friends 
    return recommended_friends 


################
# MAIN PROGRAM #
################

conf = SparkConf()
sc = SparkContext(conf=conf)

# Read text file 
network = sc.textFile(sys.argv[1]).filter(lambda obj: "\t" in obj)

user_and_friends = parseData(network)

# Maps every line into key-value pairs of ((user, friend), value)
pairs = user_and_friends.flatMap(users_to_friend)
pairs.cache()

# Get the mutual friend count for a pair of users that aren't already friends 
# Filter out pairs of users that are already friends
user_Friends = pairs.groupByKey() \
                .mapValues(sumMutualFriends) \
                .filter(lambda value: value[1] > 0)  #Filter out direct Friendships

# Reorganize the structure of each pair of users and their mutual friendship count
top_recommendations = user_Friends.map(lambda pair: (pair[0][0], (pair[0][1], pair[1])))

# Get the list of most recommended friends for each user
result = top_recommendations.groupByKey().mapValues(sortRecs)

# Sort keys
result_collect = result.collect()
sorted_result = sorted(result_collect, key=getKey)

# Save final result into text file
result.saveAsTextFile("las")

# Save sorted results into csv file
#createCSV(sorted_result, "output_sorted")

sc.stop





