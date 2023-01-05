from collections import Counter

def word_order_find(count,arr_input):
    arr=[]
    output = ""
    for i in range(count):
        arr.append(arr_input[i])
    my_dict=Counter(arr)
    for i in my_dict.values():
        output=output+str(i)+" "
    return output+str(len(set(arr)))