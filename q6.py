from pyspark import SparkContext, SparkConf
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from random import randrange
from pyspark.sql import SparkSession
import numpy as np
from pyspark.mllib.linalg import Matrices


def g(x):  # used for testing
    print(x)


if __name__ == "__main__":
    conf = SparkConf().setAppName("q6").setMaster("local")
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)
    # matrix1: 200 x 100
    # matrix2: 100 x 100
    m1Count = 200
    m2Count = 100
    inner = 100
    matrix1Rows = sc.textFile("matrix1 - small.txt").map(
        lambda x: x.split(' ')[:inner]).map(lambda x: [int(i) for i in x]).collect()
    toParallelize = [0]
    for i in range(1, len(matrix1Rows)):
        toParallelize.append(i)
    matrix1 = sc.parallelize(toParallelize).map(
        lambda x: IndexedRow(x, [x] + matrix1Rows[x]))
    matrix1_matrix = IndexedRowMatrix(matrix1)

    matrix2 = [1] + [0] * m2Count
    matrix2Rows = sc.textFile("matrix2 - small.txt").map(
        lambda x: x.split(' ')[:inner]).map(lambda x: [int(i) for i in x]).collect()
    matrix2Rows = np.array(matrix2Rows).T.tolist()
    for row in matrix2Rows:
        matrix2 += [0]
        for i in row:
            matrix2 += [i]
    matrix2_matrix = Matrices.dense(m2Count + 1, m2Count + 1, matrix2)

    multiplied = matrix1_matrix.multiply(matrix2_matrix)
    multiplied.rows.map(lambda x: x.vector.toArray().tolist()).map(
        lambda x: (x[1:])).map(lambda x: [int(i) for i in x]).map(lambda x: ' '.join(str(i) for i in x)).coalesce(1).saveAsTextFile("q6")
