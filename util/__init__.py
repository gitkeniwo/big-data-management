from . import *
import numpy as np

def aggregate_variance(v1: list, v2: list, v3: list) -> float:
    lenList = len(v1)
    sumList = []
    for i in range(0, lenList):
        sumList.append(v1[i] + v2[i] + v3[i])
    return np.var(sumList)

#def aggregate_variance(v1: list, v2: list, v3: list) -> float:
#    return np.var([np.sum(x) for x in zip(v1, v2, v3)])    # the np.sum() is slow (WHY???)

#def aggregate_variance(v1: list, v2: list, v3: list) -> float:
#    return np.var(list(map(sum, zip(v1, v2, v3))))         # Error: map() and sum() are standard operation for Spark, conflict with Python map() and sum()

def custom_sum(lst):
    total = 0
    for item in lst:
        total += item
    return total