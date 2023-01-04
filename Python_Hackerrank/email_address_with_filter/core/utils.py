import re

def fun(s):
    # return True if s is a valid email, else return False
    return bool(re.fullmatch('[A-Za-z0-9\-\_]+\@[A-Za-z0-9]+\.[A-Za-z]{2,3}', s))

def filter_mail(emails):
    return list(filter(fun, emails))