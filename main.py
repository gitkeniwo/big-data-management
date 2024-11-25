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

from util import aggregate_variance, custom_sum
from util.spark_utils import get_spark_context

from answers import q1a, q1b
from answers.q3 import q3
from answers.q4 import q4


if __name__ == '__main__':

    start_time = datetime.now()

    # on_server = False  # TODO: Set this to true if and only if deploying to the server
    on_server = True

    spark_context = get_spark_context(on_server)

    # data_frame = q1a(spark_context, on_server)

    rdd = q1b(spark_context, on_server)

    q3_result = q3(spark_context, rdd)

    q4_result = q4(spark_context, rdd)

    end_time = datetime.now()

    print("***********************************************")
    print(f"Execution time: {end_time - start_time}")
    print("***********************************************")    

    spark_context.stop()

