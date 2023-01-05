import numpy
numpy.set_printoptions(legacy='1.13')

def find_floor_ceil_rint(input):
    my_list = input
    output_list = []
    output_list.append(numpy.floor(my_list))
    output_list.append(numpy.ceil(my_list))
    output_list.append(numpy.rint(my_list))
    return output_list

