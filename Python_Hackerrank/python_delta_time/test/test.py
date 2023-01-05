import unittest
from Python_Hackerrank.python_delta_time.core.utils import *

class unitTestCase(unittest.TestCase):
    def testDeltaTime(self):
        t1 = "Sun 10 May 2015 13:54:36 -0700"
        t2 = "Sun 10 May 2015 13:54:36 -0000"
        actua_output = time_delta(t1, t2)
        expected_output = "25200"
        self.assertEqual(actua_output,expected_output)

    def testDeltaTime1(self):
        t1 = "Sat 02 May 2015 19:54:36 +0530"
        t2 = "Fri 01 May 2015 13:54:36 -0000"
        actua_output = time_delta(t1, t2)
        expected_output = "88200"
        self.assertEqual(actua_output,expected_output)