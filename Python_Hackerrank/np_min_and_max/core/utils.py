import numpy

def min_max_np(input,input2):
    dim_1, dim_2 = input.split()
    my_list = []
    for i in range(int(dim_1)):
        my_list.append(list(map(int, input2.split())))

    max_num = numpy.min(numpy.array(my_list), axis=1)
    print(numpy.max(max_num))