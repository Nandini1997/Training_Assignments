def find_python_list(input1):
    n = int(input1)
    my_list = []
    for i in range(n):
        input_string = input().split()

        if 'insert' in input_string:
            my_list.insert(int(input_string[1]), int(input_string[2]))
        if 'print' in input_string:
            print(my_list)
        if 'remove' in input_string:
            my_list.remove(int(input_string[1]))
        if 'append' in input_string:
            my_list.append(int(input_string[1]))
        if 'sort' in input_string:
            my_list.sort()
        if 'pop' in input_string:
            my_list.pop()
        if 'reverse' in input_string:
            my_list.reverse()