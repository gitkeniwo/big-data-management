from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import split, col, size
from typing import List, Tuple
import hashlib
import numpy as np
from math import ceil, log
from builtins import min
from pyspark.sql.functions import col, pow, struct
from pyspark.ml.linalg import Vectors
from pyspark.ml.linalg import Vectors, DenseVector

import numpy as np
from datetime import datetime

from util.aggregation_utils import inner_product, update_cms

def q3(spark_context: SparkContext, rdd: RDD):
    """
    q3: Paritioning-Broadcasting-Filtering process to optimize the aggregation of each triple of vectors.

    Parameters
    ----------
    spark_context : SparkContext
    rdd : RDD
    """

    # NumPartition = 32
    # NumPartition = 8     # for server (2 workers, each work has 40 cores, so 80 cores in total)
    NumPartition = 240
    
    taus = [20, 410]
    tau = spark_context.broadcast(taus)


    # obtain keyRDD which contains keys of the vector triples 
    keys = rdd.keys()
    keys2 = keys.cartesian(keys)
    keys2 = keys2.filter(lambda x: x[0] < x[1])
    keys3 = keys2.cartesian(keys)
    keys3 = keys3.filter(lambda x: x[0][1] < x[1] and x[0][0] < x[1])
    keyRDD = keys3.repartition(NumPartition)


    # create an rdd that contains original vectors, variance and average for each vector
    var_ag_rdd = rdd.map(lambda x: (x[0], x[1], np.var(x[1]), np.average(x[1])))
    vectors = var_ag_rdd.collect()
    my_dict = {item[0]: item[1:] for item in vectors}
    broadcast_vectors = spark_context.broadcast(my_dict)


    # compute second term rdd
    # print(keys2.take(10))
    l = len(rdd.first()[1])
    # print(f"vector length: {l}")
    # second_term_rdd = keys2.map(lambda x: (x, [(1/l) * 
    #                                         sum([2 * a * b for a, b in 
    #                                              zip(broadcast_vectors.value[x[0]][0], 
    #                                                  broadcast_vectors.value[x[1]][0])])]))    
    
    # #print(second_term_rdd.take(10))
    # secondterm = second_term_rdd.collect()
    # secondterm = dict(secondterm)
    # broadcast_secondterm = spark_context.broadcast(secondterm) 


    # get rid of original vectors list and create a new rdd
    var_ag_rdd2 = var_ag_rdd.map(lambda x: (x[0], x[2], x[3]))
    rows = var_ag_rdd2.collect()
    my_dict2 = {item[0]: item[1:] for item in rows}
    broadcast_var_agg = spark_context.broadcast(my_dict2)

    l = spark_context.broadcast(len(rdd.first()[1]))
    result = keyRDD.map(lambda x: (x,   
                            (broadcast_var_agg.value[x[0][0]][0]) + 
                            (broadcast_var_agg.value[x[0][1]][0]) + 
                            (broadcast_var_agg.value[x[1]][0]) +
                            ( 
                                2 * inner_product(broadcast_vectors.value[x[0][0]][0], broadcast_vectors.value[x[0][1]][0]) +
                                2 * inner_product(broadcast_vectors.value[x[0][0]][0], broadcast_vectors.value[x[1]][0]) +
                                2 * inner_product(broadcast_vectors.value[x[0][1]][0], broadcast_vectors.value[x[1]][0]) 
                            ) / l.value -
                            (2 * broadcast_var_agg.value[x[0][0]][1] * broadcast_var_agg.value[x[0][1]][1]) - 
                            (2 * broadcast_var_agg.value[x[0][0]][1] * broadcast_var_agg.value[x[1]][1]) - 
                            (2 * broadcast_var_agg.value[x[0][1]][1] * broadcast_var_agg.value[x[1]][1])) )
    
    # broadcast_secondterm.value[(x[0][0], x[0][1])] +
    # broadcast_secondterm.value[(x[0][0], x[1])] +
    # broadcast_secondterm.value[(x[0][1], x[1])] -

    result = result.filter(lambda x: x[1] <= tau.value[1])

    #print(f"result.first(): {result.first()}")
    print(f"result.count(): {result.count()}")

    return