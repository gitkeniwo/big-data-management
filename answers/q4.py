from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
from math import ceil, log
from pyspark.ml.linalg import Vectors, DenseVector

import numpy as np
from datetime import datetime


def q4(spark_context: SparkContext, rdd: RDD):
    """
    q4: Use Count-Min Sketch to represent the original vectors, 
    then for all triples of vectors <X,Y,Z> from the dataset, 
    estimate the aggregation of each triple, 
    and finally estimate the variances of each triple inside Spark.

    Parameters
    ----------
    spark_context : SparkContext
    rdd : RDD
    """

    NumPartition = 8  # for server (2 workers, each work has 40 cores, so 80 cores in total)
    taus = [20, 410]
    tau = spark_context.broadcast(taus)
    vectors = rdd.collect()
    vectors_dict = dict(vectors)
    broadcast_vectors = spark_context.broadcast(
        vectors_dict)  # broadcast this list of vector ID and respective elements to all executors.

    # cartesian join the keys 
    keys = rdd.keys()
    keys2 = keys.cartesian(keys)
    keys2 = keys2.filter(lambda x: x[0] < x[1])
    keys3 = keys2.cartesian(keys)
    keys3 = keys3.filter(lambda x: x[0][1] < x[1] and x[0][0] < x[1])

    keyRDD = keys3.repartition(NumPartition)
    # print("Number of partitions: ", keyRDD.getNumPartitions())

    print(f"vector count = {len(vectors)}")
    print(f"vectors[0]: {vectors[0]}")
    print(f"rdd.first(): {rdd.first()}")
    print(f"keyRDD.first(): {keyRDD.first()}")

    ### Parameters for Count-Min Sketch
    # f1: ε={0.001, 0.01}, δ=0.1
    # f2: ε={0.0001, 0.001, 0.002, 0.01}, δ=0.1
    e = 2.718
    epsilon = 0.001
    delta = 0.1

    w = ceil(e / epsilon)
    d = ceil(log(1 / delta))
    print(f"w = {w}, d = {d}")
    # 3 rows, 2718 cols
    # d = 3, 3 hash functions

    # sketch = np.zeros((d, w))   
    # for v in vectors:
    #     update_cms(sketch, v[1], w)
    # print(sketch)

    vectors = [v[1] for v in vectors]

    # Convert the list of vectors into a DataFrame
    df = SparkSession.createDataFrame([(Vectors.dense(v),) for v in vectors], ["features"])

    return df
