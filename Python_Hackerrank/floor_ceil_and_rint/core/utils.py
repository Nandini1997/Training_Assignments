import numpy
numpy.set_printoptions(legacy='1.13')

def find_floor_ceil_rint(input):
    my_list = list(map(float, input.split()))
    print(numpy.floor(my_list))
    print(numpy.ceil(my_list))
    print(numpy.rint(my_list))

