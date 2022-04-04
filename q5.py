from pyspark import SparkContext, SparkConf


def g(x):  # used for testing
    print(x)


if __name__ == "__main__":
    conf = SparkConf().setAppName("q5").setMaster("local")
    sc = SparkContext(conf=conf)
    reviews = sc.textFile("review.csv").map(
        lambda x: x.split("::")).map(lambda x: (x[1], 1))
    users = sc.textFile("user.csv").map(
        lambda x: x.split("::")).map(lambda x: (x[0], x[1]))
    counts = reviews.reduceByKey(lambda x, y: x+y)
    totalCount = counts.map(lambda x: (1, x[1])).reduceByKey(
        lambda x, y: x+y).collect()[0][1]
    percentages = counts.map(lambda x: (x[0], x[1]/totalCount * 100))
    top10 = percentages.sortBy(lambda x: x[1]).take(10)
    top10Parallelized = sc.parallelize(top10)
    names = top10Parallelized.join(users)
    names.map(lambda x: f'{x[1][1]}\t{x[1][0]}%').coalesce(
        1).saveAsTextFile("q5")
