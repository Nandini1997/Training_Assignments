from collections import Counter

def word_order_find(count,arr_input):
    arr=[]
    for i in range(count):
        arr.append(arr_input[i])
    print(len(set(arr)))
    my_dict=Counter(arr)
    for i in my_dict.values():
        print(i,end=" ")