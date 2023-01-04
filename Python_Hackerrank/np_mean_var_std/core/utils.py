import numpy


def mean(arr):
    print(numpy.mean(numpy.array(arr), axis=1))

def var(arr):
    print(numpy.var(numpy.array(arr), axis=0))

def std(arr):
    print(round(numpy.std(numpy.array(arr), axis=None), 11))