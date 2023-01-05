import unittest
from Python_Hackerrank.merge_the_tools.core.utils import *

class unitTestCase(unittest.TestCase):
    def testMergeTools(self):
        string, k = "AABCAAADA", 3
        actual_output = merge_the_tools(string, k)
        expected_output = ['AB', 'CA', 'AD']
        self.assertEqual(actual_output,expected_output)


    def testMergeTools2(self):
        string, k = "AABCAAADAHGAUIAAAKLA", 3
        actual_output = merge_the_tools(string, k)
        expected_output = ['AB', 'CA', 'AD', 'HGA', 'UIA', 'AK', 'LA']
        self.assertEqual(actual_output,expected_output)