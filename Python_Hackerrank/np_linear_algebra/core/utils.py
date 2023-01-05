import numpy

def np_linear_algebra(input1,input2):
    num = input1
    arr_list = []
    for i in range(num):
        arr_list.append(input2[i])

    input_list = numpy.array(arr_list, dtype=float)
    return (round(numpy.linalg.det(input_list), 2))