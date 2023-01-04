from datetime import datetime


def time_delta(t1,t2):
    time1 = datetime.strptime(t1, "%a %d %b %Y %H:%M:%S %z")
    time2 = datetime.strptime(t2, "%a %d %b %Y %H:%M:%S %z")
    return (str(abs(time1 - time2).total_seconds())).split(".")[0]