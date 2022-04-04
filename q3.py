from pyspark import SparkContext, SparkConf


def g(x):  # used for testing
    print(x)


if __name__ == "__main__":
    conf = SparkConf().setAppName("q3").setMaster("local")
    sc = SparkContext(conf=conf)
    reviews = sc.textFile("review.csv").map(lambda x: x.split("::"))
    businesses = sc.textFile("business.csv").map(
        lambda x: x.split("::"))
    users = sc.textFile("user.csv").map(lambda x: x.split("::"))
    stanford_businesseses = businesses.filter(
        lambda x: "Stanford, CA" in x[1]).map(lambda x: (x[0], x[1])).distinct()  # (business_id, address)
    reviews_businesses = reviews.map(lambda x: (
        x[2], (x[1], x[3])))  # (business_id, (user_id, stars))
    users_ids = users.map(lambda x: (x[0], x[1]))  # (user_id, name)
    join1 = reviews_businesses.join(
        stanford_businesseses).map(lambda x: x[1][0])
    join2 = users_ids.join(join1).map(lambda x: x[1])
    join2.map(lambda x: f'{x[0]}\t{x[1]}').coalesce(1).saveAsTextFile("q3")
