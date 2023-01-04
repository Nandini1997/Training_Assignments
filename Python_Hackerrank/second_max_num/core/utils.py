def second_max_num(n, arr):
    score_list = set(arr)
    max_score = max(score_list)
    score_list.remove(max_score)
    print(max(score_list))
