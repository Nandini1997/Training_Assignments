def merge_the_tools(string, k):
    my_list=[]
    output_list=""
    for i in range(0,len(string),k):
        my_list.append(string[i:i+k])

    for i in my_list:
        for j in i:
            if j not in output_list:
                output_list=output_list+j
        print(output_list)
        output_list=""