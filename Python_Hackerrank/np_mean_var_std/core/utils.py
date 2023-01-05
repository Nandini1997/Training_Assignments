import numpy


def mean(arr):
    return(numpy.mean(numpy.array(arr), axis=1))

def var(arr):
    return (numpy.var(numpy.array(arr), axis=0))

def std(arr):
    return (round(numpy.std(numpy.array(arr), axis=None), 11))