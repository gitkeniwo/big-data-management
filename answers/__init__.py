from . import *

import os
import pandas as pd
import numpy as np
from datetime import datetime

from pyspark import SparkConf, SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import split, col, size
from typing import List, Tuple
import numpy as np
from math import ceil, log
from builtins import min
from pyspark.sql.functions import col, pow, struct
from pyspark.ml.linalg import Vectors
from pyspark.ml.linalg import Vectors, DenseVector


def q1a(spark_context: SparkContext, on_server: bool) -> DataFrame:
    """
    q1a: Importing the file at {@code vectors_file_path} into a DataFrame.

    Parameters
    ----------
    spark_context : SparkContext
    on_server : bool

    Returns
    -------
    DataFrame
    """
    
    start_time = datetime.now()
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"
    spark_session = SparkSession(spark_context) 
    
    # Read CSV file into DataFrame
    df1 = spark_session.read.option("header", "false") \
        .csv(vectors_file_path)

    df2 = df1.withColumnRenamed("_c0", "k").withColumnRenamed("_c1", "v")
    
    # df3 = df2.select(split(col("v"),";")).alias("vs").drop("v")
    df3 = df2.select("k", split("v",";").alias("v"))
    #df4 = df3.select("k", col("v").cast("int"))
    #df4.printSchema()

    df_size = df3.select(size("v").alias("v"))
    nb_columns = df_size.agg(max("v")).collect()[0][0]
    
    split_df = df3.select("k", *[df3["v"][i] for i in range(nb_columns)])

    #cols = ["v[0]", "v[1]", "v[2]", "v[3]", "v[4]"]
    cols = ["v[{}]".format(x) for x in range(0, 5)]     ### !!! CHANGE TO 10000 !!! ###
    print("cols: ", cols)
    df = split_df.select("k", *(col(c).cast("int") for c in cols))

    #print("Excerpt of the dataframe content:")
    #df.show(10)

    #print("Dataframe's schema:")
    #df.printSchema()

    end_time = datetime.now()
    #print('Duration (q1a): {}'.format(end_time - start_time))
    print('Duration (q1a): {:.2f} seconds'.format((end_time - start_time).total_seconds()))

    return df


def q1b(spark_context: SparkContext, on_server: bool) -> RDD:
    """
    q1b: Import the file at {@code vectors_file_path} into an RDD.

    Parameters
    ----------
    spark_context : SparkContext
    on_server : bool

    Returns
    -------
    RDD
    """
    vectors_file_path = "/vectors.csv" if on_server else "vectors.csv"

    # Read the CSV file into an RDD of strings
    vectors_rdd01 = spark_context.textFile(vectors_file_path)

    # Split each line by comma and convert the values to integers
    vectors_rdd02 = vectors_rdd01.map(lambda line: tuple(line.strip().split(',')))
    vectors_rdd = vectors_rdd02.map(lambda x: (x[0], [int(val) for val in x[1].split(';')]))

    return vectors_rdd