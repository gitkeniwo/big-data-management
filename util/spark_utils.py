import os
from pyspark import SparkConf, SparkContext


def get_spark_context(on_server) -> SparkContext:
    """
    get_spark_context: Get a SparkContext

    Parameters
    ----------
    on_server

    Returns
    -------

    """

    spark_conf = SparkConf().setAppName("2AMD15")
    if not on_server:
        spark_conf = spark_conf.setMaster("local[*]")
    spark_context = SparkContext.getOrCreate(spark_conf)
    spark_context.setLogLevel("ERROR")

    if on_server:
        # TODO: You may want to change ERROR to WARN to receive more info.
        #  For larger data sets, to not set the
        #  log level to anything below WARN, Spark will print too much information.
        # spark_context.setLogLevel("ERROR")
        spark_context.setLogLevel("WARN")

    return spark_context
