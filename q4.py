from pyspark import SparkContext, SparkConf


def g(x):  # used for testing
    print(x)


if __name__ == "__main__":
    conf = SparkConf().setAppName("q4").setMaster("local")
    sc = SparkContext(conf=conf)
    reviews = sc.textFile("review.csv").map(
        lambda x: x.split("::")).map(lambda x: (x[2], 1))
    businesses = sc.textFile("business.csv").map(
        lambda x: x.split("::")).map(lambda x: (x[0], (x[1], x[2]))).distinct()
    counts = reviews.reduceByKey(
        lambda x, y: x+y)
    top10 = counts.sortBy(lambda x: -x[1]).take(10)
    top10Parallelized = sc.parallelize(top10).join(
        businesses).sortBy(lambda x: -x[1][0])
    top10Parallelized.map(lambda x: f'{x[0]}\t{x[1][1][0]}\t{x[1][1][1]}\t{x[1][0]}').coalesce(
        1).saveAsTextFile("q4")
