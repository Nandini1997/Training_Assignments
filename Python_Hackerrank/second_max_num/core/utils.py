def second_max_num(n, arr):
    score_list = set(arr)
    max_score = max(score_list)
    score_list.remove(max_score)
    return (max(score_list))
