from Python_Hackerrank.email_address_with_filter.core.utils import *

num = 3
email = ['brian-23@hackerrank.com', 'britts_54@hackerrank.com', 'lara@hackerrank.com']
emails=[]
for i in range(3):
    emails.append(email[i])

filtered_emails = filter_mail(emails)
filtered_emails.sort()
print(filtered_emails)