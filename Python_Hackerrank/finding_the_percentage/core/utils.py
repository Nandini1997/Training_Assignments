def finding_percentage(student_mark,name):
    total = 0
    range_iter = len(student_mark[name])
    for i in range(range_iter):
       total = total + student_mark[name][i]
    avg = total /range_iter
    return  avg