from itertools import combinations

def find_iterator_anditerables(number, string1, slice_index):
    output_list=[]
    string=string1.split()
    combination_str= list(combinations(string,slice_index))
    for i in combination_str:
        if 'a' in i:
            output_list.append(list(i))
    print(round(len(output_list)/len(combination_str),4))