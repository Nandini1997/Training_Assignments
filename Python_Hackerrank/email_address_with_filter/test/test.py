from Python_Hackerrank.email_address_with_filter.core.utils import *
import unittest

class unitTestCase(unittest.TestCase):
    def testEmailAddress(self):
        email = ['brian-23@hackerrank.com', 'britts_54@hackerrank.com', 'lara@hackerrank.com']
        emails = []
        for i in range(3):
            emails.append(email[i])
        filtered_emails = filter_mail(emails)
        actual_output = filtered_emails
        expected = ['brian-23@hackerrank.com', 'britts_54@hackerrank.com', 'lara@hackerrank.com']
        self.assertEqual(actual_output, expected)

    def testEmailAddress2(self):
        email = ['nandini@gmail.com', 'opp@gmail.com', 'res#@gmail.com']
        emails = []
        for i in range(3):
            emails.append(email[i])
        filtered_emails = filter_mail(emails)
        actual_output = filtered_emails
        expected = ['nandini@gmail.com', 'opp@gmail.com']
        self.assertEqual(actual_output, expected)
