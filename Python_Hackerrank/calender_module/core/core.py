from datetime import datetime
import calendar

def findDay(input):
    datetime_is = datetime.strptime(input, '%m %d %Y')
    return (calendar.day_name[datetime_is.weekday()].upper())