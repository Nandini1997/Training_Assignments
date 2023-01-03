from datetime import datetime
import calendar

def findDay(input):
    datetime_is = datetime.strptime(input, '%m %d %Y')
    print(calendar.day_name[datetime_is.weekday()].upper())