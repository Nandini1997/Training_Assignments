import unittest
from Python_Hackerrank.iterables_and_iterator.core.utils import *

class unitTestCases(unittest.TestCase):
    def testIterables(self):
        number = 4
        string = "a a c d"
        slice_index = 2
        actual_output = find_iterator_anditerables(number, string, slice_index)
        expected_output = 0.8333
        self.assertEqual(actual_output,expected_output)

    def testIterables2(self):
        number = 4
        string = "a a c a a a d"
        slice_index = 2
        actual_output = find_iterator_anditerables(number, string, slice_index)
        expected_output = 0.9524
        self.assertEqual(actual_output,expected_output)