from pyspark import SparkContext, SparkConf


def g(x):  # used for testing
    print(x)


def mutuals(inp):
    user = inp[0].strip()
    friends = inp[1]
    if user != '':
        pairs = []
        for friend in friends:
            friend = friend.strip()
            if friend != '':
                pair = (friend + "," + user, set(friends)) if int(friend) < int(
                    user) else (user + "," + friend, set(friends))
                pairs.append(pair)
        return pairs


if __name__ == "__main__":
    conf = SparkConf().setAppName("q1").setMaster("local")
    sc = SparkContext(conf=conf)
    user_friends = sc.textFile("mutual.txt").map(lambda x: x.split(
        "\t")).filter(lambda x: len(x) == 2).map(lambda x: [x[0], x[1].split(",")])
    friend_pairs = user_friends.flatMap(mutuals)
    common_friends = friend_pairs.reduceByKey(
        lambda x, y: x.intersection(y)).sortByKey()
    common_friends.filter(lambda x: len(x[1]) > 0).map(lambda x: f'{x[0]}\t {len(x[1])}').coalesce(
        1).saveAsTextFile("q1")
