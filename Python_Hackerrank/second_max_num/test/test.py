import unittest
from Python_Hackerrank.second_max_num.core.utils import *

class unitTestCase(unittest.TestCase):
    def testSecondMaxNum(self):
        n = 5
        arr = [2, 3, 6, 6, 5]
        expected_output = second_max_num(n, arr)
        actual_output = 5
        self.assertEqual(expected_output,actual_output)

    def testSecondMaxNum1(self):
        n = 5
        arr = [2, 4, 7, 6, 6, 5]
        expected_output = second_max_num(n, arr)
        actual_output = 6
        self.assertEqual(expected_output,actual_output)